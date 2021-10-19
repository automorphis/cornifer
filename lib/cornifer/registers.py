"""
    Cornifer, an intuitive data manager for empirical and computational mathematics.
    Copyright (C) 2021 Michael P. Lane

    This program is free software: you can redistribute it and/or modify
    it under the terms of the GNU General Public License as published by
    the Free Software Foundation, either version 3 of the License, or
    (at your option) any later version.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU General Public License for more details.
"""

import inspect
from contextlib import contextmanager
from itertools import chain
from pathlib import Path
from abc import ABC, abstractmethod

import plyvel

from cornifer.errors import Sub_Register_Cycle_Error, Data_Not_Found_Error, LevelDB_Error, \
    Data_Not_Dumped_Error, Register_Not_Open_Error, Register_Already_Open_Error
from cornifer.sequences import Sequence_Description, Sequence
from cornifer.utilities import intervals_overlap, random_unique_filename, leveldb_has_key

#################################
#         LEVELDB KEYS          #

_REGISTER_LEVELDB_NAME = "register"
_KEY_SEP = b"\x00\x00"
_KEY_SEP_LEN = len(_KEY_SEP)
_START_N_PREFIX_KEY = b"start_n_prefix"
_CLS_KEY = b"cls"
_MSG_KEY = b"msg"
_SUB_KEY_PREFIX = b"sub" + _KEY_SEP
_SEQ_KEY_PREFIX = b"seq" + _KEY_SEP
_SUB_KEY_PREFIX_LEN = len(_SUB_KEY_PREFIX)

class Register(ABC):

    #################################
    #        ERROR MESSAGES         #

    ___GETITEM___ERROR_MSG = (
"""
Acceptable syntax is, for example:
   reg[descr, 5]
   reg[descr, 10:20]
   reg[descr, 10:20, True]
   reg[descr, 10:20:3, True]
   reg[descr, 10:20:-3, True]
where `descr` is an instance of `Sequence_Description`. The optional third parameter tells 
the register whether to search recursively for the requested data; the default value, 
`False`, means that the register will not. Negative indices are not permitted, so you 
cannot do the following:
   reg[descr, -5]
   reg[descr, -5:-10:-1]
"""
    )

    _ADD_DISK_SEQ_ERROR_MSG = (
        "The `add_disk_seq` failed. The `seq` has not been dumped to disk, nor has it been linked with this "+
        "register."
    )

    #################################
    #     PUBLIC INITIALIZATION     #

    def __init__(self, saves_directory, msg):
        self.saves_directory = Path(saves_directory)

        if not self.saves_directory.is_file():
            raise FileNotFoundError(
                f"You must create the file `{str(self.saves_directory)}` before calling "+
                f"`{type(self)}({str(self.saves_directory)})`."
            )

        if not isinstance(msg, str):
            raise TypeError(f"The `msg` argument must be a `str`. Passed type of `msg`: `{str(type(msg))}`.")

        self._msg = msg
        self._msg_bytes = self._msg.encode()

        self._local_dir = None
        self._reg_file = None

        self._local_dir_bytes = None
        self._subreg_bytes = None
        self._reg_cls_bytes = type(self).__name__.encode()
        self._msg_bytes = str(self).encode()
        self._start_n_prefix = 1
        self._start_n_prefix_bytes = str(1).encode("ASCII")

        self._ram_seqs = {}
        self._db = None

        self._created = False
        self._opened = False

    @staticmethod
    def add_subclass(subclass):
        if not inspect.isclass(subclass):
            raise TypeError("The `subclass` argument must be a class.")
        if not issubclass(subclass, Register):
            raise TypeError(f"The class `{subclass.__name__}` must be a subclass of `Register`.")
        Register._constructors[subclass.__name__] = subclass

    #################################
    #     PROTEC INITIALIZATION     #

    _constructors = {}

    _instances = {}

    @staticmethod
    def _from_name(name, local_dir):

        if name == "Register":
            raise ValueError(
                "`Register` is an abstract class, meaning that `Register` itself cannot be instantiated, " +
                "only its concrete subclasses."
            )
        try:
            reg = Register._constructors[name](local_dir.parent)
        except KeyError:
            raise ValueError(
                f"`Register` is not aware of a subclass called \"{name}\". Please add the subclass to " +
                f"`Register` via `Register.add_subclass({name})`."
            )
        reg._set_local_dir(local_dir)
        return Register._get_instance(reg)

    @staticmethod
    def _add_instance(reg):
        Register._instances[reg] = reg

    @staticmethod
    def _get_instance(reg):
        try:
            return Register._instances[reg]
        except KeyError:
            Register._add_instance(reg)
            return reg

    #################################
    #    PUBLIC REGISTER METHODS    #

    def __eq__(self, other):
        return self._local_dir.resolve() == other._local_dir.resolve()

    def __hash__(self):
        return hash(str(self._local_dir.resolve()))

    def __str__(self):
        return self._msg

    def set_message(self, msg):
        self._check_open_raise("set_msg")
        self._msg = msg
        self._msg_bytes = self._msg.encode()
        self._db.put(_MSG_KEY, self._msg_bytes)

    def set_start_n_prefix(self, prefix):

        self._check_open_raise("set_start_n_prefix")
        self._start_n_prefix = prefix
        self._start_n_prefix_bytes = str(prefix).encode("ASCII")
        self._db.put(_START_N_PREFIX_KEY, self._start_n_prefix_bytes)

    @contextmanager
    def open(self):
        if not self._created:
            self._set_local_dir(random_unique_filename(self.saves_directory))
            Path.mkdir(self._local_dir)
            self._db = plyvel.DB(self._reg_file, True)
            self._opened = True
            with self._db.write_batch(transaction = True) as wb:
                wb.put(_CLS_KEY, self._reg_cls_bytes)
                wb.put(_MSG_KEY, self._msg_bytes)
                wb.put(_START_N_PREFIX_KEY, self._start_n_prefix_bytes)
            Register._add_instance(self)
            yiel = self
        else:
            yiel = self._open_created()
        try:
            yield yiel
        finally:
            yiel._close_created()

    #################################
    #    PROTEC REGISTER METHODS    #

    def _open_created(self):
        ret = Register._get_instance(self)
        if ret._opened:
            raise Register_Already_Open_Error()
        ret._opened = True
        ret._db = plyvel.DB(ret._reg_file)
        return ret

    def _close_created(self):
        self._opened = False
        self._db.close()

    @contextmanager
    def _recursive_open(self):
        if not self._created:
            yield None
        else:
            try:
                opened = self._open_created()
                need_close = True
            except Register_Already_Open_Error:
                opened = self
                need_close = False
            try:
                yield opened
            finally:
                if need_close:
                    opened._close_created()

    def _check_open_raise(self, method_name):
        if not self._opened:
            raise Register_Not_Open_Error(method_name)

    def _set_local_dir(self, filename):
        self._created = True
        self._local_dir = filename
        self._local_dir_bytes = self._local_dir.encode()
        self._reg_file = self._local_dir / _REGISTER_LEVELDB_NAME
        self._subreg_bytes = (
            _SUB_KEY_PREFIX + self._local_dir_bytes
        )

    #################################
    #     PUBLIC DESCR METHODS      #

    def get_all_descriptions(self, recursively = False):

        ret = set()
        for descr, _, _ in self._iter_all_ram_sequence_metadatas():
            ret.add(descr)

        self._check_open_raise("get_descrs")
        for descr, _, _, _ in self._iter_all_disk_seq_metadatas():
            ret.add(descr)

        if recursively:
            for subreg in self._iter_subregisters():
                with subreg._recursive_open() as subreg:
                    ret.update(subreg.get_all_descriptions())

        return ret

    #################################
    #     PROTEC DESCR METHODS      #

    #################################
    #  PUBLIC SUB-REGISTER METHODS  #

    def add_subregister(self, subreg):

        self._check_open_raise("add_subregister")

        key = subreg._get_subregister_key()
        if not leveldb_has_key(self._db, key):
            if subreg._check_no_cycles(self):
                self._db.put(key, subreg._reg_cls_bytes)
            else:
                raise Sub_Register_Cycle_Error(self, subreg)

    def remove_subregister(self, subreg):

        self._check_open_raise("remove_subregister")

        key = subreg._get_subregister_key()
        if not leveldb_has_key(self._db, key):
            self._db.remove(key)

    #################################
    #  PROTEC SUB-REGISTER METHODS  #

    def _check_no_cycles(self, original):

        if not self._created:
            return False

        with self._recursive_open() as reg:

            if any(
                original == subreg
                for subreg in reg._iter_subregisters()
            ):
                return False

            if all(
                subreg._check_no_cycles(original)
                for subreg in reg._iter_subregisters()
            ):
                return True

    def _iter_subregisters(self):
        length = len(_SUB_KEY_PREFIX)
        it = self._db.iterator(prefix = _SUB_KEY_PREFIX)
        try:
            for key, val in it:
                cls_name = self._db.get(key).encode("ASCII")
                filename = key[length:].encode("ASCII")
                subreg = Register._from_name(cls_name, filename)
                yield subreg
        finally:
            it.close()

    def _get_subregister_key(self):
        return _SUB_KEY_PREFIX + self._local_dir_bytes

    #################################
    #    PUBLIC DISK SEQ METHODS    #

    @classmethod
    @abstractmethod
    def dump_disk_data(cls, data, filename):
        """Dump data to the disk.

        This method should not change any properties of any `Register`, which is why it is a classmethod and
        not an instancemethod. It merely takes `data` and dumps it to disk.

        Most use-cases prefer the instancemethod `add_disk_seq`.

        :param data: (any type) The raw data to dump.
        :param filename: (type `pathlib.Path`) The filename to dump to. You may edit this filename if
        necessary (such as by adding a suffix), but you must return the edited filename.
        :raises Data_Not_Dumped_Error: If the dump fails for any reason.
        :return: (type `pathlib.Path) The actual filename of the data on the disk.
        """

    @classmethod
    @abstractmethod
    def load_disk_data(cls, filename):
        """Load raw data from the disk.

        This method should not change any properties of any `Register`, which is why it is a classmethod and
        not an instancemethod. It merely loads the raw data saved on the disk and returns it.

        Most use-cases prefer the method `get_seq` (which also returns RAM sequences).

        :param filename: (type `pathlib.Path`) Where to load the sequence from. You may need to edit this
        filename if necessary, such as by adding a suffix. You do not need to return the edited filename.
        :raises Data_Not_Found_Error: If the data could not be loaded because it doesn't exist.
        :raises Data_Not_Loaded_Error: If the data exists but couldn't be loaded for any reason.
        :return: (any type) The data loaded from the disk.
        """

    def add_disk_sequence(self, seq):
        """Dump a sequence to disk and link it with this `Register`.

        :param seq: (type `Sequence`)
        """

        self._check_open_raise("add_disk_seq")

        key = self._get_disk_data_key(seq.get_descr(), seq.get_start_n(), len(seq))
        if not leveldb_has_key(self._db, key):

            filename = random_unique_filename(self._local_dir)
            try:
                filename = type(self).dump_disk_data(seq, filename)
            except Data_Not_Dumped_Error:
                raise Data_Not_Dumped_Error(Register._ADD_DISK_SEQ_ERROR_MSG)

            filename_bytes = filename.encode()
            try:
                self._db.put(key, filename_bytes)
            except plyvel.Error:
                Path.unlink(filename, missing_ok=True)
                raise LevelDB_Error(
                    self._reg_file,
                    Register._ADD_DISK_SEQ_ERROR_MSG
                )

    def remove_disk_sequence(self, descr, start_n, length):

        self._check_open_raise("remove_disk_seq")

        key = self._get_disk_data_key(descr, start_n, length)
        if leveldb_has_key(self._db, key):
            self._db.remove(key)

    #################################
    #    PROTEC DISK SEQ METHODS    #

    def _get_disk_data_key(self, descr, start_n, length):
        return (
            _SUB_KEY_PREFIX                             +
            descr.to_json().                     encode("ASCII") + _KEY_SEP +
            str(start_n % self._start_n_prefix). encode("ASCII") + _KEY_SEP +
            str(length).                         encode("ASCII")
        )

    def _iter_disk_sequence_metadatas_from_description(self, descr):
        descr_bytes = descr.to_json().encode("ASCII")
        prefix = _SUB_KEY_PREFIX + descr_bytes + _KEY_SEP
        it = self._db.iterator(prefix = prefix)
        try:
            for key,val in it:
                yield self._convert_disk_sequence_key(key, descr_bytes) + (val,)
        finally:
            it.close()

    def _iter_all_disk_seq_metadatas(self):
        it = self._db.iterator(prefix = _SUB_KEY_PREFIX)
        try:
            for key,val in it:
                yield self._convert_disk_sequence_key(key) + (val,)
        finally:
            it.close()

    def _convert_disk_sequence_key(self, key, omit_descr_bytes = None):

        if omit_descr_bytes is not None:
            start_index = _SUB_KEY_PREFIX_LEN + len(omit_descr_bytes) + _KEY_SEP_LEN
        else:
            start_index = _SUB_KEY_PREFIX_LEN

        key_split = key[start_index:].split(_KEY_SEP)

        if omit_descr_bytes is not None:
            start_n_bytes, length_bytes = key_split
            descr_bytes = omit_descr_bytes
        else:
            descr_bytes, start_n_bytes, length_bytes = key_split

        return (
            descr_bytes.decode("ASCII"),
            self._start_n_prefix + int(start_n_bytes.decode("ASCII")),
            int(length_bytes.decode("ASCII"))
        )

    #################################
    #    PUBLIC RAM SEQ METHODS     #

    def add_ram_sequence(self, seq):

        descr = seq.get_descr()
        start_n = seq.get_start_n()
        if (descr,start_n) not in self._ram_seqs.keys():
            self._ram_seqs[(descr, start_n)] = seq

    def remove_ram_sequence(self, seq):

        try:
            del self._ram_seqs[(seq.get_descr(), seq.get_start_n())]
        except KeyError:
            pass

    #################################
    #    PROTEC RAM SEQ METHODS     #

    def _iter_all_ram_sequence_metadatas(self):
        for (descr, start_n), seq in self._ram_seqs.items():
            yield descr, start_n, len(seq)

    #################################
    # PUBLIC RAM & DISK SEQ METHODS #

    def get_sequence(self, descr, n, recursively = False):

        for _ram_seq in self._ram_seqs:
            if _ram_seq.get_descr() == descr and n in _ram_seq:
                return _ram_seq

        self._check_open_raise("get_seq")
        for descr, start_n, length, filename in self._iter_disk_sequence_metadatas_from_description(descr):
            if start_n <= n < start_n + length:
                data = type(self).load_disk_data(filename)
                return Sequence(data, descr, start_n)

        if recursively:
            for subreg in self._iter_subregisters():
                with subreg._recursive_open() as subreg:
                    try:
                        return subreg.get_sequence(descr, n, True)
                    except Data_Not_Found_Error:
                        pass

        raise Data_Not_Found_Error

    def get_all_sequences(self, descr, recursively = False):

        for (_descr, _), _ram_seq in self._ram_seqs.items():
            if _descr == descr:
                yield _ram_seq

        self._check_open_raise("get_all_seqs")
        for (_descr, _start_n, _, filename) in self._iter_all_disk_seq_metadatas():
            if _descr == descr:
                data = type(self).load_disk_data(filename)
                yield Sequence(data, _descr, _start_n)

        if recursively:
            for subreg in self._iter_subregisters():
                with subreg._recursive_open() as subreg:
                    for seq in subreg.get_all_sequences():
                        yield seq

    def __getitem__(self, descr_and_n_and_recursively):
        short = descr_and_n_and_recursively

        # check that the general shape and type of `descr_and_n_and_recursively` is correct
        if (
            isinstance(short, tuple) or
            not(2 <= len(short) <= 3) or
            not(isinstance(short[0], Sequence_Description)) or
            not(isinstance(short[1], (int,slice))) or
            (len(short) == 3 and not isinstance(short[2],bool))
        ):
            raise TypeError(Register.___GETITEM___ERROR_MSG)

        # check that slices do not have negative indices
        if (
            isinstance(short[1], slice) and (
                (short.start is not None and short.start < 0) or
                (short.stop  is not None and short.stop  < 0)
            )
        ):
            raise ValueError(Register.___GETITEM___ERROR_MSG)

        # unpack
        if len(short) == 2:
            descr, n = descr_and_n_and_recursively
            recursively = False
        else:
            descr, n, recursively = descr_and_n_and_recursively

        # return iterator if given slice
        if isinstance(n, slice):
            return _Seq_Iter(self, descr, n, recursively)

        # otherwise return a single element
        else:
            return self.get_sequence(descr, n, recursively)[n]

    def list_sequences_calculated(self, recursively = False):

        ret = {}
        for descr in self.get_all_descriptions(recursively):
            intervals_sorted = sorted(
                [
                    (start_n, length)
                    for _descr, start_n, length in self._iter_ram_and_disk_sequence_metadatas(recursively)
                    if descr == _descr
                ],
                key = lambda t: t[0]
            )
            intervals_reduced = []
            for int1 in intervals_sorted:
                for i, int2 in enumerate(intervals_reduced):
                    if intervals_overlap(int1,int2):
                        a1, l1 = int1
                        a2, l2 = int2
                        if a2 + l2 < a1 + l1:
                            intervals_reduced[i] = (a2, a1 + l1 - a2)
                            break
                else:
                    intervals_reduced.append(int1)
            ret[descr] = intervals_reduced
        return ret

    #################################
    # PROTEC RAM & DISK SEQ METHODS #

    def _iter_ram_and_disk_sequence_metadatas(self, recursively = False):

        for metadata in chain(self._iter_all_ram_sequence_metadatas(), self._iter_all_disk_seq_metadatas()):
            yield metadata

        if recursively:
            for subreg in self._iter_subregisters():
                with subreg._recursive_open() as subreg:
                    for metadata in subreg._iter_ram_and_disk_sequence_metadatas():
                        yield metadata

class Pickle_Register(Register):

    @classmethod
    def dump_disk_data(cls, data, filename):
        pass

    @classmethod
    def load_disk_data(cls, filename):
        pass

Register.add_subclass(Pickle_Register)

class NumPy_Register(Register):

    @classmethod
    def dump_disk_data(cls, data, filename):
        pass

    @classmethod
    def load_disk_data(cls, filename):
        pass

Register.add_subclass(NumPy_Register)

class RAM_Only_Register(Register):

    @classmethod
    def dump_disk_data(cls, data, filename):
        pass

    @classmethod
    def load_disk_data(cls, filename):
        pass

Register.add_subclass(RAM_Only_Register)

class Plaintext_Register(Register):

    @classmethod
    def dump_disk_data(cls, data, filename):
        pass

    @classmethod
    def load_disk_data(cls, filename):
        pass

Register.add_subclass(Plaintext_Register)

class _Seq_Iter:

    def __init__(self, reg, descr, slc, recursively = False):
        self.reg = reg
        self.descr = descr
        self.slc = slice(
            slc.start if slc.start else 0,
            slc.stop,
            slc.step  if slc.step  else 1
         )
        self.recursively = recursively
        self.curr_n = self.slc.start
        self.curr_seq = None

    def __iter__(self):
        return self

    def __next__(self):
        if self.slc.stop is not None and self.curr_n >= self.slc.stop:
            raise StopIteration
        if not self.curr_seq or self.curr_n not in self.curr_seq:
            self.curr_seq = self.reg.get_sequence(self.descr, self.curr_n, self.recursively)
        ret = self.curr_seq[self.curr_n]
        self.curr_n += self.slc.step
        return ret