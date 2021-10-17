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
import json
from contextlib import contextmanager
from itertools import chain
from pathlib import Path
from abc import ABC, abstractmethod

import plyvel

from cornifer.errors import Sub_Register_Cycle_Error, Data_Not_Found_Error, LevelDB_Error, \
    Data_Not_Dumped_Error, Register_Not_Open_Error, Register_Already_Open_Error
from cornifer.sequences import Sequence_Description
from cornifer.utilities import intervals_overlap, random_unique_filename, open_leveldb, leveldb_has_key

_REGISTER_LEVELDB_NAME = "register"

class Register(ABC):

    #################################
    #        ERROR MESSAGES         #

    ___GETITEM___ERROR_MSG = (
"""
Acceptable syntax is, for example:
   register[descr, 5]
   register[descr, 10:20]
   register[descr, 10:20, True]
   register[descr, 10:20:3, True]
   register[descr, 10:20:-3, True]
where `descr` is an instance of `Sequence_Description`. The optional third parameter tells 
the register whether to search recursively for the requested data; the default value, 
`False`, means that the register will not. Negative indices are not permitted, so you 
cannot do the following:
   register[descr, -5]
   register[descr, -5:-10:-1]
"""
    )

    _ADD_DISK_SEQ_ERROR_MSG = (
        "The `add_disk_seq` failed. The `seq` has not been dumped to disk, nor has it been linked with this "+
        "regiser."
    )

    #################################
    #         LEVELDB KEYS          #

    _KEY_SEP =            b"\x00\x00"
    _START_N_PREFIX_KEY = b"start_n_prefix"
    _CLS_KEY =            b"cls"
    _MSG_KEY =            b"msg"
    _SUB_KEY_PREFIX =     b"sub" + _KEY_SEP
    _SEQ_KEY_PREFIX =     b"seq" + _KEY_SEP

    #################################
    #     PUBLIC INITIALIZATION     #

    def __init__(self, saves_directory, msg):
        self.saves_directory = Path(saves_directory)

        if not self.saves_directory.is_file():
            raise FileNotFoundError(
                f"You must create the file `{str(self.saves_directory)}` before calling "+
                f"`{type(self)}({str(self.saves_directory)})`."
            )

        if msg is not None and not isinstance(msg, str):
            raise TypeError(f"The `msg` argument must be a `str`. Passed type of `msg`: `{str(type(msg))}`.")

        self._msg = msg

        self._local_dir = None
        self._register_file = None

        self._local_dir_bytes = None
        self._sub_register_bytes = None
        self._register_cls_bytes = type(self).__name__.encode()
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

    @staticmethod
    def _from_name(name, local_dir):
        if name == "Register":
            raise ValueError(
                "`Register` is an abstract class, meaning that `Register` itself cannot be instantiated, " +
                "only its concrete subclasses."
            )
        try:
            register = Register._constructors[name](local_dir.parent)
        except KeyError:
            raise ValueError(
                f"`Register` is not aware of a subclass called \"{name}\". Please add the subclass to " +
                f"`Register` via `Register.add_subclass({name})`."
            )
        register._set_local_dir(local_dir)
        return Register._instances.get(register, register)

    _instances = {}

    #################################
    #    PUBLIC REGISTER METHODS    #

    def __eq__(self, other):
        return self._local_dir.resolve() == other._local_dir.resolve()

    def __hash__(self):
        return hash(str(self._local_dir.resolve()))

    def __str__(self):
        if self._msg is not None:
            return self._msg
        else:
            self._check_open_raise("__str__")
            self._msg = self._db.get(Register._MSG_KEY)
            return self._msg.encode("ASCII")

    def set_msg(self, msg):
        self._check_open_raise("set_msg")
        self._msg = msg
        self._msg_bytes = self._msg.encode()
        self._db.put(Register._MSG_KEY, self._msg_bytes)

    def set_start_n_prefix(self, prefix):

        self._check_open_raise("set_start_n_prefix")
        self._start_n_prefix = prefix
        self._start_n_prefix_bytes = str(prefix).encode("ASCII")
        self._db.put(Register._START_N_PREFIX_KEY, self._start_n_prefix_bytes)

    @contextmanager
    def open(self):
        if not self._created:
            self._set_local_dir(random_unique_filename(self.saves_directory))
            Path.mkdir(self._local_dir)
            self._db = plyvel.DB(self._register_file, True)
            self._opened = True
            with self._db.write_batch(transaction = True) as wb:
                wb.put(Register._CLS_KEY, self._register_cls_bytes)
                wb.put(Register._MSG_KEY, self._msg_bytes)
                wb.put(Register._START_N_PREFIX_KEY, self._start_n_prefix_bytes)
            Register._instances[self] = self
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
        ret = Register._instances.get(self, self)
        if ret._opened:
            raise Register_Already_Open_Error()
        ret._opened = True
        ret._db = plyvel.DB(ret._register_file)
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
                need_close = False
            try:
                yield None
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
        self._register_file = self._local_dir / _REGISTER_LEVELDB_NAME
        self._sub_register_bytes = (
            Register._SUB_KEY_PREFIX + self._local_dir_bytes
        )

    #################################
    #     PUBLIC DESCR METHODS      #

    def get_descrs(self, recursively = False):
        self._check_open_raise("get_descrs")
        descrs = set(descr for descr,_ in self._disk_seqs.keys())
        if recursively:
            for sub_register in self._sub_registers:
                descrs.update(sub_register.get_descrs(True))
        return descrs

    #################################
    #     PROTEC DESCR METHODS      #

    #################################
    #  PUBLIC SUB-REGISTER METHODS  #

    def add_sub_register(self, register):

        self._check_open_raise("add_sub_register")

        key = register._get_sub_register_key()
        if not leveldb_has_key(self._db, key):
            if register._check_no_cycles(self):
                self._db.put(key, register._register_cls_bytes)
            else:
                raise Sub_Register_Cycle_Error(self, register)

    def remove_sub_register(self, register):

        self._check_open_raise("remove_sub_register")

        key = register._get_sub_register_key()
        if not leveldb_has_key(self._db, key):
            self._db.remove(key)

    #################################
    #  PROTEC SUB-REGISTER METHODS  #

    def _check_no_cycles(self, original):

        if not self._created:
            return False

        with self._recursive_open():
            if any(original == sub_register for sub_register in self._iter_sub_registers()):
                return False
            if all(sub_register._check_no_cycles(original) for sub_register in self._iter_sub_registers()):
                return True

    def _iter_sub_registers(self):
        length = len(Register._SUB_KEY_PREFIX)
        it = self._db.iterator(prefix = Register._SUB_KEY_PREFIX)
        for key, val in it:
            cls_name = self._db.get(key).encode("ASCII")
            filename = key[length:].encode("ASCII")
            register = Register._from_name(cls_name, filename)
            yield register
        it.close()

    def _get_sub_register_key(self):
        return Register._SUB_KEY_PREFIX + self._local_dir_bytes

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

    def add_disk_seq(self, seq):
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
                    self._register_file,
                    Register._ADD_DISK_SEQ_ERROR_MSG
                )

    def remove_disk_seq(self, descr, start_n, length):

        self._check_open_raise("remove_disk_seq")
        key = self._get_disk_data_key(descr, start_n, length)
        if leveldb_has_key(self._db, key):
            self._db.remove(key)

    #################################
    #    PROTEC DISK SEQ METHODS    #

    def _get_disk_data_key(self, descr, start_n, length):
        return (
            descr.to_json().                     encode("ASCII") + Register._KEY_SEP +
            str(start_n % self._start_n_prefix). encode("ASCII") + Register._KEY_SEP +
            str(length).                         encode("ASCII")
        )

    #################################
    #    PUBLIC RAM SEQ METHODS     #

    def add_ram_seq(self, seq):

        descr = seq.get_descr()
        start_n = seq.get_start_n()
        if (descr,start_n) not in self._ram_seqs.keys():
            self._ram_seqs[(descr, start_n)] = seq

    def remove_ram_seq(self, seq):

        try:
            del self._ram_seqs[(seq.get_descr(), seq.get_start_n())]
        except KeyError:
            pass

    #################################
    #    PROTEC RAM SEQ METHODS     #

    #################################
    # PUBLIC RAM & DISK SEQ METHODS #

    def get_seq(self, descr, n, recursively = True):

        for _ram_seq in self._ram_seqs:
            if _ram_seq.get_descr() == descr and n in _ram_seq:
                return _ram_seq

        self._load()
        for (_descr,_start_n,_length), _filename in self._disk_seqs.items():
            if _descr == descr and _start_n <= n < _start_n + _length:
                return type(self).load_disk_data(_filename)

        if recursively:
            for _sub_register in self._sub_registers:
                try:
                    return _sub_register.get_seq(descr, n, True)
                except Data_Not_Found_Error:
                    pass

        raise Data_Not_Found_Error

    def __getitem__(self, descr_and_n_and_recursively):
        short = descr_and_n_and_recursively
        if (
            isinstance(short, tuple) or
            not(2 <= len(short) <= 3) or
            not(isinstance(short[0], Sequence_Description)) or
            not(isinstance(short[1], (int,slice))) or
            (len(short) == 3 and not isinstance(short[2],bool))
        ):
            raise TypeError(Register.___GETITEM___ERROR_MSG)

        if (
            isinstance(short[1], slice) and (
                (short.start is not None and short.start < 0) or
                (short.stop  is not None and short.stop  < 0)
            )
        ):
            raise ValueError(Register.___GETITEM___ERROR_MSG)

        if len(short) == 2:
            descr, n = descr_and_n_and_recursively
            recursively = False
        else:
            descr, n, recursively = descr_and_n_and_recursively

        if isinstance(n, slice):
            return _Seq_Iter(self, descr, n, recursively)
        else:
            return self.get_seq(descr, n, recursively)[n]

    def list_seqs_calculated(self, recursively = False):

        ret = {}
        for descr in self.get_descrs(recursively):
            intervals_sorted = sorted(
                [
                    (start_n, length)
                    for _descr, start_n, length in self._get_ram_and_disk_metadatas(recursively)
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

    def _get_ram_and_disk_metadatas(self, recursively = False):
        self._load()
        metadatas = set()
        if recursively:
            for register in self._sub_registers:
                metadatas.update(register._get_ram_and_disk_metadatas(True))
        metadatas.update(chain(self._ram_seqs.keys(), self._disk_seqs.keys()))
        return metadatas

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

class _Seq_Iter:

    def __init__(self, register, descr, slc, recursively = False):
        self.register = register
        self.descr = descr
        self.slc = slice(
            slc.start if slc.start else 0,
            slc.stop,
            slc.step  if slc.step  else 1
         )
        self.recursively = recursively
        self.curr_n = slc.start if slc.start else 0
        self.curr_seq = None

    def __iter__(self):
        return self

    def __next__(self):
        if self.slc.stop is not None and self.curr_n >= self.slc.stop:
            raise StopIteration
        if not self.curr_seq or self.curr_n not in self.curr_seq:
            self.curr_seq = self.register.get_seq(self.descr,self.curr_n,self.recursively)
        ret = self.curr_seq[self.curr_n]
        self.curr_n += self.slc.step
        return ret