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
import math
import pickle
from contextlib import contextmanager
from itertools import chain
from pathlib import Path
from abc import ABC, abstractmethod

import numpy as np
import plyvel

from cornifer.errors import Sub_Register_Cycle_Error, Data_Not_Found_Error, LevelDB_Error, \
    Data_Not_Dumped_Error, Register_Not_Open_Error, Register_Already_Open_Error
from cornifer.sequences import Sequence_Description, Block
from cornifer.utilities import intervals_overlap, random_unique_filename, leveldb_has_key, \
    leveldb_prefix_iterator

#################################
#         LEVELDB KEYS          #

_REGISTER_LEVELDB_NAME   = "register"
_KEY_SEP                 = b"\x00\x00"
_START_N_MAGN_KEY        = b"start_n_magn"
_START_N_RES_LEN_KEY     = b"start_n_res_len"
_CLS_KEY                 = b"cls"
_MSG_KEY                 = b"msg"
_SUB_KEY_PREFIX          = b"sub"
_BLK_KEY_PREFIX          = b"blk"
_DESCR_ID_KEY_PREFIX     = b"descr"
_ID_DESCR_KEY_PREFIX     = b"id"

_KEY_SEP_LEN             = len(_KEY_SEP)
_SUB_KEY_PREFIX_LEN      = len(_SUB_KEY_PREFIX)
_BLK_KEY_PREFIX_LEN      = len(_BLK_KEY_PREFIX)
_DESCR_ID_KEY_PREFIX_LEN = len(_DESCR_ID_KEY_PREFIX)
_ID_DESCR_KEY_PREFIX_LEN = len(_ID_DESCR_KEY_PREFIX)

class Register(ABC):

    #################################
    #           CONSTANTS           #

    _START_N_RES_LEN_DEFAULT = 12

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

    _ADD_DISK_BLOCK_ERROR_MSG = (
        "The `add_disk_block` failed. The `blk` has not been dumped to disk, nor has it been linked with "
        "this register."
    )

    _LOCAL_DIR_ERROR_MSG = "The `Register` database could not be found."

    _SET_START_N_MAGNITUDE_ERROR_MSG = "This `Register` has not been changed."

    _SET_START_N_RESIDUE_LENGTH_ERROR_MSG = "This `Register` has not been changed."

    #################################
    #     PUBLIC INITIALIZATION     #

    def __init__(self, saves_directory, msg):
        self.saves_directory = Path(saves_directory)

        if not self.saves_directory.is_file():
            raise FileNotFoundError(
                f"You must create the file `{str(self.saves_directory)}` before calling "+
                f"`{self.__class__.__name__}({str(self.saves_directory)})`."
            )

        elif not isinstance(msg, str):
            raise TypeError(f"The `msg` argument must be a `str`. Passed type of `msg`: `{str(type(msg))}`.")

        self._msg = msg
        try:
            self._msg_bytes = self._msg.encode("ASCII")
        except UnicodeEncodeError:
            raise ValueError(
                "The passed message includes non-ASCII characters."
            )

        self._local_dir = None
        self._reg_file = None
        self._local_dir_bytes = None
        self._subreg_bytes = None
        self._reg_cls_bytes = type(self).__name__.encode()

        self._start_n_magn = 0
        self._start_n_magn_bytes = b"0"
        self._start_n_res_len = Register._START_N_RES_LEN_DEFAULT
        self._start_n_res_len_bytes = str(self._start_n_res_len).encode("ASCII")

        self._ram_blks = {}
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

    def set_start_n_magnitude(self, magnitude):

        self._check_open_raise("set_start_n_magnitude")

        if not isinstance(magnitude, int):
            raise TypeError("`magnitude` must be an `int`.")
        elif magnitude < 0:
            raise ValueError("`magnitude` must be non-negative.")

        if magnitude > self._start_n_magn:
            dif = magnitude - self._start_n_magn
            zeroes = b"0"*dif
            with leveldb_prefix_iterator(self._db, _BLK_KEY_PREFIX) as it:
                for key,_ in it:
                    _, start_n_bytes, _ = Register._split_disk_block_key(key)
                    if start_n_bytes[-dif:] != zeroes:
                        raise ValueError(
                            f"Cannot set `start_n_magnitude` to {magnitude} because that number is "+
                            "too large."
                        )
        if magnitude != self._start_n_magn:
            dif = magnitude - self._start_n_magn
            if dif < 0:
                zeroes = b"0"*(-dif)
            with self._db.snapshot() as sn:
                with leveldb_prefix_iterator(sn, _BLK_KEY_PREFIX) as it:
                    try:
                        with self._db.write_batch(transaction = True) as wb:
                            for key,val in it:
                                descr_json, start_n_bytes, length_bytes = Register._split_disk_block_key(key)
                                if dif > 0:
                                    start_n_bytes = start_n_bytes[ : -dif ]
                                elif dif < 0:
                                    start_n_bytes = start_n_bytes + zeroes
                                new_key = Register._join_disk_block_metadata(
                                    descr_json, start_n_bytes, length_bytes
                                )
                                wb.put(new_key, val)
                    except plyvel.Error:
                        raise LevelDB_Error(
                            self._reg_file,
                            Register._SET_START_N_MAGNITUDE_ERROR_MSG
                        )

            self._start_n_magn = magnitude
            self._start_n_magn_bytes = str(magnitude).encode("ASCII")
            try:
                self._db.put(_START_N_MAGN_KEY, self._start_n_magn_bytes)
            except plyvel.Error:
                raise LevelDB_Error(
                    self._reg_file,
                    Register._SET_START_N_MAGNITUDE_ERROR_MSG
                )

    def set_start_n_residue_length(self, residue_length):

        self._check_open_raise("set_start_n_residue_length")

        if residue_length < self._start_n_res_len:
            dif = self._start_n_res_len - residue_length
            zeroes = b"0"*dif
            with leveldb_prefix_iterator(self._db, _BLK_KEY_PREFIX) as it:
                for key,_ in it:
                    _, start_n_bytes, _ = Register._split_disk_block_key(key)
                    if start_n_bytes[:dif] != zeroes:
                        raise ValueError(
                            f"Cannot set `start_n_residue_length` to {residue_length} because that number is"+
                            "too small."
                        )

        if residue_length != self._start_n_res_len:
            dif = self._start_n_res_len - residue_length
            if dif < 0:
                zeroes = b"0"*(-dif)
            with self._db.snapshot() as sn:
                with leveldb_prefix_iterator(sn, _BLK_KEY_PREFIX) as it:
                    try:
                        with self._db.write_batch(transaction = True) as wb:
                            for key,val in it:
                                descr_json, start_n_bytes, length_bytes = Register._split_disk_block_key(key)
                                if dif > 0:
                                    start_n_bytes = start_n_bytes[ dif : ]
                                elif dif < 0:
                                    start_n_bytes = start_n_bytes + zeroes
                                new_key = Register._join_disk_block_metadata(
                                    descr_json, start_n_bytes, length_bytes
                                )
                                wb.put(new_key, val)
                    except plyvel.Error:
                        raise LevelDB_Error(
                            self._reg_file,
                            Register._SET_START_N_RESIDUE_LENGTH_ERROR_MSG
                        )

            self._start_n_res_len = residue_length
            self._start_n_res_len_bytes = str(self._start_n_res_len).encode("ASCII")
            try:
                self._db.put(_START_N_RES_LEN_KEY, self._start_n_res_len_bytes)
            except plyvel.Error:
                raise LevelDB_Error(
                    self._reg_file,
                    Register._SET_START_N_RESIDUE_LENGTH_ERROR_MSG
                )

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
                wb.put(_START_N_MAGN_KEY, self._start_n_magn_bytes)
                wb.put(_START_N_RES_LEN_KEY, self._start_n_res_len_bytes)
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
        if not self._local_dir.is_dir():
            raise FileNotFoundError(Register._LOCAL_DIR_ERROR_MSG)

    def _set_local_dir(self, filename):
        if not filename.is_dir():
            raise FileNotFoundError(Register._LOCAL_DIR_ERROR_MSG)
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
        for descr, _, _ in self._iter_ram_block_metadatas():
            ret.add(descr)

        self._check_open_raise("get_all_descriptions")
        with leveldb_prefix_iterator(self._db, _ID_DESCR_KEY_PREFIX) as it:
            for _, val in it:
                ret.add(Sequence_Description.from_json(val.decode("ASCII")))

        if recursively:
            for subreg in self._iter_subregisters():
                with subreg._recursive_open() as subreg:
                    ret.update(subreg.get_all_descriptions())

        return ret

    #################################
    #     PROTEC DESCR METHODS      #

    def _get_descr_by_id(self, _id):
        return self._db.get(_ID_DESCR_KEY_PREFIX + _id)

    def _get_id_by_descr(self, descr, descr_json):
        if descr is not None:
            return self._db.get(_DESCR_ID_KEY_PREFIX + descr.to_json().encode("ASCII"))
        elif descr_json is not None:
            return self._db.get(_DESCR_ID_KEY_PREFIX + descr_json)
        else:
            raise ValueError

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
        with leveldb_prefix_iterator(self._db, _SUB_KEY_PREFIX) as it:
            for key, val in it:
                cls_name = self._db.get(key).encode("ASCII")
                filename = key[length:].encode("ASCII")
                subreg = Register._from_name(cls_name, filename)
                yield subreg

    def _get_subregister_key(self):
        return _SUB_KEY_PREFIX + self._local_dir_bytes

    #################################
    #    PUBLIC DISK BLK METHODS    #

    @classmethod
    @abstractmethod
    def dump_disk_data(cls, data, filename):
        """Dump data to the disk.

        This method should not change any properties of any `Register`, which is why it is a classmethod and
        not an instancemethod. It merely takes `data` and dumps it to disk.

        Most use-cases prefer the instancemethod `add_disk_block`.

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

        Most use-cases prefer the method `get_disk_block`.

        :param filename: (type `pathlib.Path`) Where to load the block from. You may need to edit this
        filename if necessary, such as by adding a suffix. You do not need to return the edited filename.
        :raises Data_Not_Found_Error: If the data could not be loaded because it doesn't exist.
        :raises Data_Not_Loaded_Error: If the data exists but couldn't be loaded for any reason.
        :return: (any type) The data loaded from the disk.
        """

    def add_disk_block(self, blk):
        """Dump a block to disk and link it with this `Register`.

        :param blk: (type `Block`)
        """

        self._check_open_raise("add_disk_block")
        res = str(blk.get_start_n() % self._start_n_magn)
        if len(res) > self._start_n_res_len:
            raise IndexError(
                "Even after accounting for the `start_n_magnitude` for this register, the `start_n` for the "+
                "passed `blk` is too large. Either increase `start_n_magnitude` (via " +
                "`reg.set_start_n_magniude`) or increase the `start_n_residue_length` " +
                "(via `reg.set_start_n_residue_length`).\n" +
                f"`start_n`                : {blk.get_start_n()}\n" +
                f"`start_n_magnitude`      : {self._start_n_magn}\n" +
                f"`start_n` residue        : {res}\n" +
                f"`start_n_residue_length` : {self._start_n_res_len}\n"
            )

        key = self._get_disk_data_key(blk.get_descr(), None, blk.get_start_n(), len(blk))
        if not leveldb_has_key(self._db, key):

            filename = random_unique_filename(self._local_dir)
            try:
                filename = type(self).dump_disk_data(blk.get_data(), filename)
            except Data_Not_Dumped_Error:
                raise Data_Not_Dumped_Error(Register._ADD_DISK_BLOCK_ERROR_MSG)

            filename_bytes = filename.encode()
            try:
                self._db.put(key, filename_bytes)
            except plyvel.Error:
                Path.unlink(filename, missing_ok=True)
                raise LevelDB_Error(
                    self._reg_file,
                    Register._ADD_DISK_BLOCK_ERROR_MSG
                )

    def remove_disk_block(self, descr, start_n, length):

        self._check_open_raise("remove_disk_block")

        key = self._get_disk_data_key(descr, None, start_n, length)
        if leveldb_has_key(self._db, key):
            filename = Path(str(self._db.get(key)))
            self._db.remove(key)
            filename.unlink() # TODO add error checks

    def get_disk_block_by_metadata(self, descr, start_n, length, recursively = False):

        self._check_open_raise("get_disk_block")
        key = self._get_disk_data_key(descr, None, start_n, length)
        if leveldb_has_key(self._db, key):
            filename = Path(self._db.get(key).decode("ASCII"))
            data = type(self).load_disk_data(filename)
            return Block(data, descr, start_n)

        if recursively:
            for subreg in self._iter_subregisters():
                with subreg._recursive_open() as subreg:
                    try:
                        return subreg.get_disk_block(descr, start_n, length, True)
                    except Data_Not_Found_Error:
                        pass

        raise Data_Not_Found_Error

    def get_disk_block_by_n(self, descr, n, recursively = False):

        for descr, start_n, length, _ in self._iter_disk_block_metadatas(descr, None):
            if start_n <= n < start_n + length:
                return self.get_disk_block_by_metadata(descr, start_n, length)

        if recursively:
            for subreg in self._iter_subregisters():
                with subreg._recursive_open() as subreg:
                    try:
                        return subreg.get_disk_block_by_n(descr, n, True)
                    except Data_Not_Found_Error:
                        pass

        raise Data_Not_Found_Error

    def get_all_disk_blocks(self, descr, recursively = False):

        self._check_open_raise("get_all_disk_blocks")
        for _, descr, start_n, _, filename in self._iter_disk_block_metadatas(descr, None):
            data = type(self).load_disk_data(filename)
            yield Block(data, descr, start_n)

        if recursively:
            for subreg in self._iter_subregisters():
                with subreg._recursive_open() as subreg:
                    for blk in subreg.get_all_blocks():
                        yield blk

    #################################
    #    PROTEC DISK BLK METHODS    #

    def _get_disk_data_key(self, descr, descr_json, start_n, length):
        if descr is not None:
            _id = self._get_id_by_descr(descr.to_json().encode("ASCII"), None)
        elif descr_json is not None:
            _id = self._get_id_by_descr(None, descr_json)
        else:
            raise ValueError
        start_n_str = str(start_n)[:-self._start_n_magn].zfill(self._start_n_res_len)
        return (
                _SUB_KEY_PREFIX             +
                _id                         + _KEY_SEP +
                start_n_str.encode("ASCII") + _KEY_SEP +
                str(length).encode("ASCII")
        )

    def _iter_disk_block_metadatas(self, descr, descr_json):

        if descr is not None:
            descr_json = descr.to_json().encode("ASCII")

        if descr_json is None:
            prefix = _SUB_KEY_PREFIX
        else:
            prefix = _SUB_KEY_PREFIX + descr_json

        with self._db.snapshot() as sn:
            with leveldb_prefix_iterator(sn, prefix) as it:
                for key,val in it:
                    yield self._convert_disk_block_key(key, descr) + (Path(val.decode("ASCII")),)

    @staticmethod
    def _split_disk_block_key(key):
        return key[:_BLK_KEY_PREFIX_LEN].split(_KEY_SEP)

    @staticmethod
    def _join_disk_block_metadata(descr_json, start_n_bytes, length_bytes):
        return (
            _BLK_KEY_PREFIX +
            descr_json      + _KEY_SEP +
            start_n_bytes   + _KEY_SEP +
            length_bytes
        )

    def _convert_disk_block_key(self, key, descr = None):

        descr_id, start_n_bytes, length_bytes = Register._split_disk_block_key(key)
        if descr is None:
            descr_json = self._get_descr_by_id(descr_id)
            descr = Sequence_Description.from_json(descr_json.decode("ASCII"))
        return descr, int(start_n_bytes.decode("ASCII")), int(length_bytes.decode("ASCII"))

    #################################
    #    PUBLIC RAM BLK METHODS     #

    def add_ram_block(self, blk):

        descr = blk.get_descr()
        start_n = blk.get_start_n()
        if (descr,start_n) not in self._ram_blks.keys():
            self._ram_blks[(descr, start_n)] = blk

    def remove_ram_block(self, blk):

        try:
            del self._ram_blks[(blk.get_descr(), blk.get_start_n())]
        except KeyError:
            pass

    def get_ram_block_by_n(self, descr, n, recursively = False):

        for _, start_n, length in self._iter_ram_block_metadatas(descr):
            if start_n <= n < start_n + length:
                return self.get_ram_block_by_metadata(descr, start_n, length)

        if recursively:
            for subreg in self._iter_subregisters():
                with subreg._recursive_open() as subreg:
                    try:
                        return subreg.get_disk_block_by_n(descr, n, True)
                    except Data_Not_Found_Error:
                        pass

        raise Data_Not_Found_Error

    def get_ram_block_by_metadata(self, descr, start_n, length, recursively = False):
        try:
            return self._ram_blks[(descr, start_n, length)]
        except KeyError:
            if recursively:
                for subreg in self._iter_subregisters():
                    with subreg._recursive_open() as subreg:
                        try:
                            return subreg.get_ram_block_by_metadata(descr, start_n, length, True)
                        except Data_Not_Found_Error:
                            pass
                raise Data_Not_Found_Error
            else:
                raise Data_Not_Found_Error

    def get_all_ram_blocks(self, descr):
        return self._ram_blks.values()

    #################################
    #    PROTEC RAM BLK METHODS     #

    def _iter_ram_block_metadatas(self, descr = None):
        for (_descr, _start_n), _blk in self._ram_blks.items():
            if descr is not None and _descr == descr:
                yield _descr, _start_n, len(_blk)

    #################################
    # PUBLIC RAM & DISK BLK METHODS #

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
            return _Element_Iter(self, descr, n, recursively)

        # otherwise return a single element
        else:
            for descr, start_n, length in self._iter_disk_block_metadatas(descr,None):
                if start_n <= n < start_n + length:
                    break
            else:
                raise Data_Not_Found_Error()
            return self.get_disk_block(descr,start_n, length, recursively)[n]

    def list_sequences_calculated(self, descr, recursively = False):

        intervals_sorted = sorted(
            [
                (start_n, length)
                for _, start_n, length, _ in self._iter_ram_and_disk_block_metadatas(descr,None,recursively)
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
        return intervals_reduced

    #################################
    # PROTEC RAM & DISK BLK METHODS #

    def _iter_ram_and_disk_block_metadatas(self, descr = None, descr_json = None, recursively = False):

        for metadata in chain(
            self._iter_ram_block_metadatas(descr),
            self._iter_disk_block_metadatas(descr,descr_json)
        ):
            yield metadata

        if recursively:
            for subreg in self._iter_subregisters():
                with subreg._recursive_open() as subreg:
                    for metadata in subreg._iter_ram_block_metadatas_from_description(descr, True):
                        yield metadata

class Pickle_Register(Register):

    @classmethod
    def dump_disk_data(cls, data, filename):
        filename = filename.with_suffix(".pkl")
        with filename.open("wb") as fh:
            pickle.dump(data, fh)
        return filename

    @classmethod
    def load_disk_data(cls, filename):
        with filename.open("rb") as fh:
            return pickle.load(fh)

Register.add_subclass(Pickle_Register)

class NumPy_Register(Register):

    @classmethod
    def dump_disk_data(cls, data, filename):
        filename = filename.with_suffix(".npy")
        np.save(filename, data, allow_pickle = False, fix_imports = False)
        return filename

    @classmethod
    def load_disk_data(cls, filename):
        return np.load(filename, mmap_mode = None, allow_pickle = False, fix_imports = False)

Register.add_subclass(NumPy_Register)

class HDF5_Register(Register):

    @classmethod
    def dump_disk_data(cls, data, filename):
        pass

    @classmethod
    def load_disk_data(cls, filename):
        pass

Register.add_subclass(HDF5_Register)

class _Element_Iter:

    def __init__(self, reg, descr, slc, recursively = False):
        self.reg = reg
        self.descr = descr
        self.step = slc.step if slc.step else 1
        self.stop = slc.stop
        self.recursively = recursively
        self.curr_blk = None
        self.intervals = None
        self.curr_n = slc.start if slc.start else 0

    def update_sequences_calculated(self):
        self.intervals = dict( self.reg.list_sequences_calculated(self.descr, self.recursively) )

    def get_next_block(self):
        try:
            return self.reg.get_ram_block_by_n(self.descr, self.curr_n, self.recursively)
        except Data_Not_Found_Error:
            return self.reg.get_disk_block_by_n(self.descr, self.curr_n, self.recursively)

    def __iter__(self):
        return self

    def __next__(self):

        if self.stop is not None and self.curr_n >= self.stop:
            raise StopIteration

        elif self.curr_blk is None:
            self.update_sequences_calculated()
            self.curr_n = max( self.intervals[0][0] , self.curr_n )
            try:
                self.curr_blk = self.get_next_block()
            except Data_Not_Found_Error:
                raise StopIteration

        elif self.curr_n not in self.curr_blk:
            try:
                self.curr_blk = self.get_next_block()
            except Data_Not_Found_Error:
                self.update_sequences_calculated()
                for start, length in self.intervals:
                    if start > self.curr_n:
                        self.curr_n += math.ceil( (start - self.curr_n) / self.step ) * self.step
                        break
                else:
                    raise StopIteration
                self.curr_blk = self.get_next_block()

        ret = self.curr_blk[self.curr_n]
        self.curr_n += self.step
        return ret
