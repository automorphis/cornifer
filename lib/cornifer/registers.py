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
from pathlib import Path
from abc import ABC, abstractmethod

import numpy as np
import plyvel

from cornifer.errors import Subregister_Cycle_Error, Data_Not_Found_Error, \
    Data_Not_Dumped_Error, Register_Not_Open_Error, Register_Already_Open_Error, Register_Not_Created_Error, \
    Critical_Database_Error, Database_Error
from cornifer.sequences import Apri_Info, Block
from cornifer.utilities import intervals_overlap, random_unique_filename, leveldb_has_key, \
    leveldb_prefix_iterator

#################################
#         LEVELDB KEYS          #

_REGISTER_LEVELDB_NAME   = "register"
_KEY_SEP                 = b"\x00\x00"
_START_N_HEAD_KEY        = b"head"
_START_N_TAIL_LENGTH_KEY = b"tail_length"
_CLS_KEY                 = b"cls"
_MSG_KEY                 = b"msg"
_SUB_KEY_PREFIX          = b"sub"
_BLK_KEY_PREFIX          = b"blk"
_APRI_ID_KEY_PREFIX      = b"apri"
_ID_APRI_KEY_PREFIX      = b"id"
_CURR_ID_KEY             = b"curr_id"

_KEY_SEP_LEN             = len(_KEY_SEP)
_SUB_KEY_PREFIX_LEN      = len(_SUB_KEY_PREFIX)
_BLK_KEY_PREFIX_LEN      = len(_BLK_KEY_PREFIX)
_APRI_ID_KEY_PREFIX_LEN  = len(_APRI_ID_KEY_PREFIX)
_ID_APRI_KEY_PREFIX_LEN  = len(_ID_APRI_KEY_PREFIX)

class Register(ABC):

    #################################
    #           CONSTANTS           #

    _START_N_TAIL_LENGTH_DEFAULT = 12

    #################################
    #        ERROR MESSAGES         #

    ___GETITEM___ERROR_MSG = (
"""
Acceptable syntax is, for example:
   reg[apri, 5]
   reg[apri, 10:20]
   reg[apri, 10:20, True]
   reg[apri, 10:20:3, True]
   reg[apri, 10:20:-3, True]
where `apri` is an instance of `Apri_Info`. The optional third parameter tells 
the register whether to search recursively for the requested data; the default value, 
`False`, means that the register will not. Negative indices are not permitted, so you 
cannot do the following:
   reg[apri, -5]
   reg[apri, -5:-10:-1]
"""
    )

    _ADD_DISK_BLOCK_ERROR_MSG = (
        "The `add_disk_block` failed. The `blk` has not been dumped to disk, nor has it been linked with "
        "this register."
    )

    _LOCAL_DIR_ERROR_MSG = "The `Register` database could not be found."

    _SET_START_N_INFO_ERROR_MSG = (
        "`set_start_n_info` failed. Recovered successfully; the database has not been changed or corrupted."
    )

    #################################
    #     PUBLIC INITIALIZATION     #

    def __init__(self, saves_directory, msg):
        self.saves_directory = Path(saves_directory)

        if not self.saves_directory.is_dir():
            raise FileNotFoundError(
                f"You must create the file `{str(self.saves_directory)}` before calling "+
                f"`{self.__class__.__name__}(\"{str(self.saves_directory)}\", \"{msg}\")`."
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

        self._start_n_head = 0
        self._start_n_tail_length = Register._START_N_TAIL_LENGTH_DEFAULT
        self._start_n_tail_mod = 10 ** self._start_n_tail_length

        self._ram_blks = []
        self._db = None

        self._created = False

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
            raise TypeError(
                "`Register` is an abstract class, meaning that `Register` itself cannot be instantiated, " +
                "only its concrete subclasses."
            )
        con = Register._constructors.get(name, None)
        if con is None:
            raise TypeError(
                f"`Register` is not aware of a subclass called `{name}`. Please add the subclass to " +
                f"`Register` via `Register.add_subclass({name})`."
            )

        reg1 = con(local_dir.parent, "")
        reg1._set_local_dir(local_dir)
        reg2 = Register._get_instance(reg1)

        if reg1 is reg2:
            # if `reg1 is reg2`, then the user does not have a `reg1` reference, hence its LevelDB
            # database is not open, hence it is safe to manually open here
            db = plyvel.DB(str(reg2._local_dir / _REGISTER_LEVELDB_NAME))
            msg_bytes = db.get(_MSG_KEY)
            reg2._msg = msg_bytes.decode("ASCII")
            reg2._msg_bytes = msg_bytes
            db.close()

        return reg2

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
        if not self._created or not other._created:
            raise Register_Not_Created_Error("__eq__")
        else:
            return type(self) == type(other) and self._local_dir.resolve() == other._local_dir.resolve()

    def __hash__(self):
        if not self._created:
            raise Register_Not_Created_Error("__hash__")
        else:
            return hash(str(self._local_dir.resolve())) + hash(type(self))

    def __str__(self):
        return self._msg

    def __repr__(self):
        return f"{self.__class__.__name__}(\"{str(self.saves_directory)}\", \"{self._msg}\")"

    def set_message(self, msg):
        self._check_open_raise("set_message")
        self._msg = msg
        self._msg_bytes = self._msg.encode()
        self._db.put(_MSG_KEY, self._msg_bytes)

    def set_start_n_info(self, head, tail_length):

        self._check_open_raise("set_start_n_info")

        if not isinstance(head, int) or not isinstance(tail_length, int):
            raise TypeError("`head` and `tail_length` must both be of type `int`.")

        elif head < 0 or tail_length <= 0:
            raise ValueError("`head` must be non-negative and and `tail_length` must be positive.")

        if head == self._start_n_head and tail_length == self._start_n_tail_length:
            return

        blank = Apri_Info()
        new_mod = 10 ** tail_length
        with leveldb_prefix_iterator(self._db, _BLK_KEY_PREFIX) as it:
            for key, _ in it:
                _, start_n, _ = self._convert_disk_block_key(key,blank)
                if start_n // new_mod != head:
                    apri,_,length = self._convert_disk_block_key(key)
                    raise ValueError(
                        "The following `start_n` does not have the correct head:\n" +
                        f"`start_n`   : {start_n}\n" +
                        "That `start_n` is associated with a `Block` whose `Apri_Info` and length is:\n" +
                        f"`Apri_Info` : {str(apri.to_json())}\n" +
                        f"length      : {length}"
                    )

        try:
            with self._db.write_batch(transaction = True) as wb:
                wb.put(_START_N_HEAD_KEY, str(head).encode("ASCII"))
                wb.put(_START_N_TAIL_LENGTH_KEY, str(tail_length).encode("ASCII"))
        except plyvel.Error:
            raise Database_Error(
                self._reg_file,
                Register._SET_START_N_INFO_ERROR_MSG
            )

        old_keys = []
        with self._db.snapshot() as sn:
            with leveldb_prefix_iterator(sn, _BLK_KEY_PREFIX) as it:
                try:
                    with self._db.write_batch(transaction = True) as wb:
                        for key,val in it:
                            _, start_n, _ = self._convert_disk_block_key(key,blank)
                            apri_json, _, length_bytes = Register._split_disk_block_key(key)
                            new_start_n_bytes = str(start_n % new_mod).encode("ASCII")
                            new_key = Register._join_disk_block_metadata(apri_json, new_start_n_bytes, length_bytes)
                            if key != new_key:
                                old_keys.append(key)
                            wb.put(new_key, val)
                except plyvel.Error:
                    try:
                        self._db.put(_START_N_HEAD_KEY, str(self._start_n_head).encode("ASCII"))
                        self._db.put(_START_N_TAIL_LENGTH_KEY, str(self._start_n_tail_length).encode("ASCII"))
                    except plyvel.Error:
                        raise Critical_Database_Error(
                            self._reg_file,
                            "Could not recover from a failed batch write."
                        )
                    raise Database_Error(
                        self._reg_file,
                        Register._SET_START_N_INFO_ERROR_MSG
                    )

        try:
            for key in old_keys:
                self._db.delete(key)
        except plyvel.Error:
            raise Critical_Database_Error(
                self._reg_file,
                "`set_start_n_info` failed."
            )

        self._start_n_head = head
        self._start_n_tail_length = tail_length
        self._start_n_tail_mod = 10 ** self._start_n_tail_length

    @contextmanager
    def open(self):
        if not self._created:
            local_dir = random_unique_filename(self.saves_directory)
            local_dir.mkdir()
            self._set_local_dir(local_dir)
            self._db = plyvel.DB(str(self._reg_file), create_if_missing= True)
            with self._db.write_batch(transaction = True) as wb:
                wb.put(_CLS_KEY, self._reg_cls_bytes)
                wb.put(_MSG_KEY, self._msg_bytes)
                wb.put(_START_N_HEAD_KEY, str(self._start_n_head).encode("ASCII"))
                wb.put(_START_N_TAIL_LENGTH_KEY, str(self._start_n_tail_length).encode("ASCII"))
                wb.put(_CURR_ID_KEY, b"0")
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
        if not ret._created:
            raise Register_Not_Created_Error("_open_created")
        if ret._db is not None and not ret._db.closed:
            raise Register_Already_Open_Error()
        ret._db = plyvel.DB(str(ret._reg_file))
        return ret

    def _close_created(self):
        self._db.close()

    @contextmanager
    def _recursive_open(self):
        if not self._created:
            raise Register_Not_Created_Error("_recursive_open")
        else:
            try:
                yiel = self._open_created()
                need_close = True
            except Register_Already_Open_Error:
                yiel = self
                need_close = False
            try:
                yield yiel
            finally:
                if need_close:
                    yiel._close_created()

    def _check_open_raise(self, method_name):
        if self._db is None or self._db.closed:
            raise Register_Not_Open_Error(method_name)
        if not self._local_dir.is_dir():
            raise FileNotFoundError(Register._LOCAL_DIR_ERROR_MSG)

    def _set_local_dir(self, local_dir):
        if not local_dir.parent.resolve() == self.saves_directory.resolve():
            raise ValueError(
                "The `local_dir` argument must be a sub-directory of `reg.saves_directory`.\n" +
                f"`local_dir.parent`    : {str(local_dir.parent)}\n"
                f"`reg.saves_directory` : {str(self.saves_directory)}"
            )
        if not local_dir.is_dir():
            raise FileNotFoundError(Register._LOCAL_DIR_ERROR_MSG)
        self._created = True
        self._local_dir = local_dir
        self._local_dir_bytes = str(self._local_dir).encode("ASCII")
        self._reg_file = self._local_dir / _REGISTER_LEVELDB_NAME
        self._subreg_bytes = (
            _SUB_KEY_PREFIX + self._local_dir_bytes
        )

    #################################
    #      PUBLIC APRI METHODS      #

    def get_all_apri_info(self, recursively = False):

        ret = set()
        for blk in self._ram_blks:
            ret.add(blk.get_apri())

        self._check_open_raise("get_all_apri_info")
        with leveldb_prefix_iterator(self._db, _ID_APRI_KEY_PREFIX) as it:
            for _, val in it:
                ret.add(Apri_Info.from_json(val.decode("ASCII")))

        if recursively:
            for subreg in self._iter_subregisters():
                with subreg._recursive_open() as subreg:
                    ret.update(subreg.get_all_apri_info())

        return ret

    #################################
    #      PROTEC APRI METHODS      #

    def _get_apri_by_id(self, _id):
        return self._db.get(_ID_APRI_KEY_PREFIX + _id)

    def _get_id_by_apri(self, apri, apri_json):

        if apri is not None:
            key = _APRI_ID_KEY_PREFIX + apri.to_json().encode("ASCII")
        elif apri_json is not None:
            key = _APRI_ID_KEY_PREFIX + apri_json
        else:
            raise ValueError

        _id = self._db.get(key, default = None)
        if _id is not None:
            return _id
        else:
            _id = self._db.get(_CURR_ID_KEY)
            next_id = str(int(_id) + 1).encode("ASCII")
            with self._db.write_batch(transaction = True) as wb:
                wb.put(_CURR_ID_KEY, next_id)
                wb.put(key, _id)
                wb.put(_ID_APRI_KEY_PREFIX + _id, key[_APRI_ID_KEY_PREFIX_LEN:])
            return _id

    #################################
    #      PUBLIC APOS METHODS      #

    def set_apos_info(self, apri, apos): pass

    def get_apos_info(self): pass

    def remove_apos_info(self, apri, apos): pass

    #################################
    #  PUBLIC SUB-REGISTER METHODS  #

    def add_subregister(self, subreg):

        self._check_open_raise("add_subregister")
        if not subreg._created:
            raise Register_Not_Created_Error("add_subregister")

        key = subreg._get_subregister_key()
        if not leveldb_has_key(self._db, key):
            if subreg._check_no_cycles(self):
                self._db.put(key, subreg._reg_cls_bytes)
            else:
                raise Subregister_Cycle_Error(self, subreg)

    def remove_subregister(self, subreg):

        self._check_open_raise("remove_subregister")

        key = subreg._get_subregister_key()
        if leveldb_has_key(self._db, key):
            self._db.delete(key)

    #################################
    #  PROTEC SUB-REGISTER METHODS  #

    def _check_no_cycles(self, original):

        if not self._created or not original._created:
            raise Register_Not_Created_Error("_check_no_cycles")

        if self is original:
            return False

        with self._recursive_open() as reg:

            if any(
                original is subreg
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
                cls_name = self._db.get(key).decode("ASCII")
                filename = Path(key[length:].decode("ASCII"))
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

        start_n_head = blk.get_start_n() // self._start_n_tail_mod
        if start_n_head != self._start_n_head :
            raise IndexError(
                "The `start_n` for the passed `Block` does not have the correct head:\n" +
                f"`tail_length`   : {self._start_n_tail_length}\n" +
                f"expected `head` : {self._start_n_head}\n"
                f"`start_n`       : {blk.get_start_n()}\n" +
                f"`start_n` head  : {start_n_head}\n"
            )

        key = self._get_disk_block_key(blk.get_apri(), None, blk.get_start_n(), len(blk))
        if not leveldb_has_key(self._db, key):

            filename = random_unique_filename(self._local_dir)
            try:
                filename = type(self).dump_disk_data(blk.get_data(), filename)
            except Data_Not_Dumped_Error:
                raise Data_Not_Dumped_Error(Register._ADD_DISK_BLOCK_ERROR_MSG)

            filename_bytes = str(filename).encode("ASCII")
            try:
                self._db.put(key, filename_bytes)
            except plyvel.Error:
                Path.unlink(filename, missing_ok=True)
                raise Database_Error(
                    self._reg_file,
                    Register._ADD_DISK_BLOCK_ERROR_MSG
                )

    def remove_disk_block(self, apri, start_n, length):

        self._check_open_raise("remove_disk_block")

        key = self._get_disk_block_key(apri, None, start_n, length)
        if leveldb_has_key(self._db, key):
            filename = Path(self._db.get(key).decode("ASCII"))
            self._db.delete(key)
            filename.unlink() # TODO add error checks

    def get_disk_block_by_metadata(self, apri, start_n, length, recursively = False):

        self._check_open_raise("get_disk_block_by_metadata")
        key = self._get_disk_block_key(apri, None, start_n, length)
        if leveldb_has_key(self._db, key):
            filename = Path(self._db.get(key).decode("ASCII"))
            data = type(self).load_disk_data(filename)
            return Block(data, apri, start_n)

        if recursively:
            for subreg in self._iter_subregisters():
                with subreg._recursive_open() as subreg:
                    try:
                        return subreg.get_disk_block(apri, start_n, length, True)
                    except Data_Not_Found_Error:
                        pass

        raise Data_Not_Found_Error

    def get_disk_block_by_n(self, apri, n, recursively = False):

        self._check_open_raise("get_disk_block_by_n")

        if n < 0:
            raise ValueError("n must be positive")

        for apri, start_n, length, _ in self._iter_disk_block_metadatas(apri, None):
            if start_n <= n < start_n + length:
                return self.get_disk_block_by_metadata(apri, start_n, length)

        if recursively:
            for subreg in self._iter_subregisters():
                with subreg._recursive_open() as subreg:
                    try:
                        return subreg.get_disk_block_by_n(apri, n, True)
                    except Data_Not_Found_Error:
                        pass

        raise Data_Not_Found_Error

    def get_all_disk_blocks(self, apri, recursively = False):

        self._check_open_raise("get_all_disk_blocks")
        for apri, start_n, _, filename in self._iter_disk_block_metadatas(apri, None):
            data = type(self).load_disk_data(filename)
            yield Block(data, apri, start_n)

        if recursively:
            for subreg in self._iter_subregisters():
                with subreg._recursive_open() as subreg:
                    for blk in subreg.get_all_blocks():
                        yield blk

    #################################
    #    PROTEC DISK BLK METHODS    #

    def _get_disk_block_key(self, apri, apri_json, start_n, length):

        if apri is None and apri_json is None:
            raise ValueError

        _id = self._get_id_by_apri(apri, apri_json)
        tail = start_n % self._start_n_tail_mod

        return (
                _BLK_KEY_PREFIX             +
                _id                         + _KEY_SEP +
                str(tail)  .encode("ASCII") + _KEY_SEP +
                str(length).encode("ASCII")
        )

    def _iter_disk_block_metadatas(self, apri, apri_json):

        if apri_json is None and apri is None:
            prefix = _BLK_KEY_PREFIX
        else:
            prefix = _BLK_KEY_PREFIX + self._get_id_by_apri(apri,apri_json)

        with self._db.snapshot() as sn:
            with leveldb_prefix_iterator(sn, prefix) as it:
                for key,val in it:
                    yield self._convert_disk_block_key(key, apri) + (Path(val.decode("ASCII")),)

    @staticmethod
    def _split_disk_block_key(key):
        return tuple(key[_BLK_KEY_PREFIX_LEN:].split(_KEY_SEP))

    @staticmethod
    def _join_disk_block_metadata(apri_json, start_n_bytes, length_bytes):
        return (
            _BLK_KEY_PREFIX +
            apri_json      + _KEY_SEP +
            start_n_bytes   + _KEY_SEP +
            length_bytes
        )

    def _convert_disk_block_key(self, key, apri = None):

        apri_id, start_n_bytes, length_bytes = Register._split_disk_block_key(key)
        if apri is None:
            apri_json = self._get_apri_by_id(apri_id)
            apri = Apri_Info.from_json(apri_json.decode("ASCII"))
        return (
            apri,
            int(start_n_bytes.decode("ASCII")) + self._start_n_head * self._start_n_tail_mod,
            int(length_bytes.decode("ASCII"))
        )

    #################################
    #    PUBLIC RAM BLK METHODS     #

    def add_ram_block(self, blk):

        if all(ram_blk is not blk for ram_blk in self._ram_blks):
            self._ram_blks.append(blk)

    def remove_ram_block(self, blk):

        for i, ram_blk in enumerate(self._ram_blks):
            if ram_blk is blk:
                del self._ram_blks[i]
                return

        raise Data_Not_Found_Error

    def get_ram_block_by_n(self, apri, n, recursively = False):

        #TODO for warnings: if ram blocks have overlapping data, log warning when this methid
        # is called

        for blk in self._ram_blks:
            start_n = blk.get_start_n()
            if blk.get_apri() == apri and start_n <= n < start_n + len(blk):
                return blk

        if recursively:
            for subreg in self._iter_subregisters():
                with subreg._recursive_open() as subreg:
                    try:
                        return subreg.get_disk_block_by_n(apri, n, True)
                    except Data_Not_Found_Error:
                        pass

        raise Data_Not_Found_Error

    def get_all_ram_blocks(self, apri, recursively = False):
        for blk in self._ram_blks:
            if blk.get_apri() == apri:
                yield blk
        if recursively:
            self._check_open_raise("get_all_ram_blocks")
            for subreg in self._iter_subregisters():
                with subreg._recursive_open() as subreg:
                    for blk in subreg.get_all_ram_blocks(apri, True):
                        yield blk

    #################################
    #    PROTEC RAM BLK METHODS     #

    #################################
    # PUBLIC RAM & DISK BLK METHODS #

    def __getitem__(self, apri_and_n_and_recursively):
        short = apri_and_n_and_recursively

        # check that the general shape and type of `apri_and_n_and_recursively` is correct
        if (
            isinstance(short, tuple) or
            not(2 <= len(short) <= 3) or
            not(isinstance(short[0], Apri_Info)) or
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
            apri, n = apri_and_n_and_recursively
            recursively = False
        else:
            apri, n, recursively = apri_and_n_and_recursively

        # return iterator if given slice
        if isinstance(n, slice):
            return _Element_Iter(self, apri, n, recursively)

        # otherwise return a single element
        else:
            for apri, start_n, length in self._iter_disk_block_metadatas(apri,None):
                if start_n <= n < start_n + length:
                    break
            else:
                raise Data_Not_Found_Error()
            return self.get_disk_block_by_metadata(apri, start_n, length, recursively)[n]

    def list_sequences_calculated(self, apri, recursively = False):

        intervals_sorted = sorted(
            [
                (start_n, length)
                for _, start_n, length, _ in self._iter_ram_and_disk_block_metadatas(apri,recursively)
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

    def _iter_ram_and_disk_block_metadatas(self, apri, recursively = False):

        for blk in self._ram_blks:
            if blk.get_apri() == apri:
                yield apri, blk.get_start_n(), len(blk)

        for metadata in self._iter_disk_block_metadatas(apri,None):
            yield metadata

        if recursively:
            for subreg in self._iter_subregisters():
                with subreg._recursive_open() as subreg:
                    for metadata in subreg._iter_ram_and_disk_block_metadatas(apri, True):
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

class Numpy_Register(Register):

    @classmethod
    def dump_disk_data(cls, data, filename):
        filename = filename.with_suffix(".npy")
        np.save(filename, data, allow_pickle = False, fix_imports = False)
        return filename

    @classmethod
    def load_disk_data(cls, filename):
        return np.load(filename, mmap_mode = None, allow_pickle = False, fix_imports = False)

Register.add_subclass(Numpy_Register)

class HDF5_Register(Register):

    @classmethod
    def dump_disk_data(cls, data, filename):
        pass

    @classmethod
    def load_disk_data(cls, filename):
        pass

Register.add_subclass(HDF5_Register)

class _Element_Iter:

    def __init__(self, reg, apri, slc, recursively = False):
        self.reg = reg
        self.apri = apri
        self.step = slc.step if slc.step else 1
        self.stop = slc.stop
        self.recursively = recursively
        self.curr_blk = None
        self.intervals = None
        self.curr_n = slc.start if slc.start else 0

    def update_sequences_calculated(self):
        self.intervals = dict( self.reg.list_sequences_calculated(self.apri, self.recursively) )

    def get_next_block(self):
        try:
            return self.reg.get_ram_block_by_n(self.apri, self.curr_n, self.recursively)
        except Data_Not_Found_Error:
            return self.reg.get_disk_block_by_n(self.apri, self.curr_n, self.recursively)

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