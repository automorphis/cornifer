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
import shutil
import warnings
import zipfile
import zlib
from contextlib import contextmanager
from pathlib import Path, PurePath
from abc import ABC, abstractmethod

import numpy as np
import plyvel

from cornifer.errors import Data_Not_Found_Error, Register_Already_Open_Error, Register_Error, Compression_Error, \
    Decompression_Error
from cornifer.info import Apri_Info, Apos_Info
from cornifer.blocks import Block
from cornifer.file_metadata import File_Metadata
from cornifer.utilities import intervals_overlap, random_unique_filename, leveldb_has_key, \
    leveldb_prefix_iterator, is_int, zip_archive_is_empty
from cornifer.register_file_structure import VERSION_FILE_NAME, REGISTER_LEVELDB_FILENAME, LOCAL_DIR_CHARS, \
    COMPRESSED_FILE_SUFFIX
from cornifer.version import CURRENT_VERSION, COMPATIBLE_VERSIONS

#################################
#         LEVELDB KEYS          #

_KEY_SEP                   = b"\x00\x00"
_START_N_HEAD_KEY          = b"head"
_START_N_TAIL_LENGTH_KEY   = b"tail_length"
_CLS_KEY                   = b"cls"
_MSG_KEY                   = b"msg"
_SUB_KEY_PREFIX            = b"sub"
_BLK_KEY_PREFIX            = b"blk"
_APRI_ID_KEY_PREFIX        = b"apri"
_ID_APRI_KEY_PREFIX        = b"id"
_CURR_ID_KEY               = b"curr_id"
_APOS_KEY_PREFIX           = b"apos"
_COMPRESSED_KEY_PREFIX     = b"compr"

_KEY_SEP_LEN               = len(_KEY_SEP)
_SUB_KEY_PREFIX_LEN        = len(_SUB_KEY_PREFIX)
_BLK_KEY_PREFIX_LEN        = len(_BLK_KEY_PREFIX)
_APRI_ID_KEY_PREFIX_LEN    = len(_APRI_ID_KEY_PREFIX)
_ID_APRI_KEY_PREFIX_LEN    = len(_ID_APRI_KEY_PREFIX)
_COMPRESSED_KEY_PREFIX_LEN = len(_COMPRESSED_KEY_PREFIX)

_IS_NOT_COMPRESSED_VAL     = b"\0"

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

    _DISK_BLOCK_DATA_NOT_FOUND_ERROR_MSG_FULL = (
        "No disk block found with the following data: {0}, start_n = {1}, length = {2}."
    )

    _DISK_BLOCK_DATA_NOT_FOUND_ERROR_MSG_N = (
        "No disk block found with the following data: {0}, n = {1}."
    )

    _NO_COMPRESSION_KEY_ERROR_MSG = (
        "Could not find compression key for the `Block` with the following data: {0}, start_n = {1}, length = {2}."
    )

    _NOT_CREATED_ERROR_MESSAGE = (
        "The `Register` database has not been created. You must do `with reg.open() as reg:` at least once before " +
        "calling the method `{0}`."
    )

    #################################
    #     PUBLIC INITIALIZATION     #

    def __init__(self, saves_directory, message):
        """Abstract `Register` constructor.

        When called by concrete subclasses, this constructor neither creates nor opens a `Register` database.

        :param saves_directory: (type `str`)
        :param message: (type `str`) A brief message describing the data associated to this `Register`.
        """

        if not isinstance(saves_directory, (str, PurePath)):
            raise TypeError("`saves_directory` must be a string or a `PurePath`.")

        self.saves_directory = Path(saves_directory)

        if not self.saves_directory.is_dir():
            raise FileNotFoundError(
                f"You must create the file `{str(self.saves_directory)}` before calling "+
                f"`{self.__class__.__name__}(\"{str(self.saves_directory)}\", \"{message}\")`."
            )

        elif not isinstance(message, str):
            raise TypeError(
                f"The `message` argument must be a string. Passed type of `message`: `{str(type(message))}`."
            )

        self._msg = message
        try:
            self._msg_bytes = self._msg.encode("ASCII")
        except UnicodeEncodeError:
            raise ValueError(
                "The passed message includes non-ASCII characters."
            )

        self._local_dir = None
        self._reg_file = None
        self._version_file = None
        self._local_dir_bytes = None
        self._subreg_bytes = None
        self._reg_cls_bytes = type(self).__name__.encode("ASCII")

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
    def _from_local_dir(local_dir):
        """
        :param local_dir: (type `pathlib.Path`)
        :return: (type `Register`)
        """

        if Register._instance_exists(local_dir):
            # return the `Register` that has already been opened
            return Register._get_instance(local_dir)

        else:

            try:
                db = plyvel.DB(str(local_dir / REGISTER_LEVELDB_FILENAME).encode("ASCII"))
            except plyvel.IOError:
                raise Register_Already_Open_Error()

            cls_name = db.get(_CLS_KEY)
            if cls_name is None:
                raise Register_Error(f"`{_CLS_KEY.decode('ASCII')}` key not found for register : {str(local_dir)}")
            cls_name = cls_name.decode("ASCII")

            if cls_name == "Register":
                raise TypeError(
                    "`Register` is an abstract class, meaning that `Register` itself cannot be instantiated, " +
                    "only its concrete subclasses."
                )

            con = Register._constructors.get(cls_name, None)
            if con is None:
                raise TypeError(
                    f"`Register` is not aware of a subclass called `{cls_name}`. Please add the subclass to "+
                    f"`Register` via `Register.add_subclass({cls_name})`."
                )

            msg_bytes = db.get(_MSG_KEY)
            if msg_bytes is None:
                raise Register_Error(f"`{_MSG_KEY.decode('ASCII')}` key not found for register : {str(local_dir)}")
            msg = msg_bytes.decode("ASCII")

            reg = con(local_dir.parent, msg)
            reg._set_local_dir(local_dir)
            return reg

    @staticmethod
    def _add_instance(local_dir, reg):
        """
        :param local_dir: (type `pathlib.Path`)
        :param reg: (type `Register`)
        """
        Register._instances[local_dir] = reg

    @staticmethod
    def _instance_exists(local_dir):
        """
        :param local_dir: (type `pathlib.Path`)
        :return: (type `bool`)
        """
        return local_dir in Register._instances.keys()

    @staticmethod
    def _get_instance(local_dir):
        """
        :param local_dir: (type `pathlib.Path`)
        :return: (type `Register`)
        """
        return Register._instances[local_dir]

    #################################
    #    PUBLIC REGISTER METHODS    #

    def __eq__(self, other):

        if not self._created or not other._created:
            raise Register_Error(Register._NOT_CREATED_ERROR_MESSAGE.format("__eq__"))

        elif type(self) != type(other):
            return False

        else:
            return self._local_dir.resolve() == other._local_dir.resolve()

    def __hash__(self):

        if not self._created:
            raise Register_Error(Register._NOT_CREATED_ERROR_MESSAGE.format("__hash__"))

        else:
            return hash(str(self._local_dir.resolve())) + hash(type(self))

    def __str__(self):
        return self._msg

    def __repr__(self):
        return f"{self.__class__.__name__}(\"{str(self.saves_directory)}\", \"{self._msg}\")"

    def __contains__(self, apri):
        self._check_open_raise("__contains__")
        return apri in self.get_all_apri_info()

    def set_message(self, message):
        """Give this `Register` a brief description.

        WARNING: This method OVERWRITES the current message. In order to append a new message to the current one, do
        something like the following:

            old_message = str(reg)
            new_message = old_message + " Hello!"
            reg.set_message(new_message)

        :param message: (type `str`)
        """

        self._check_open_raise("set_message")

        if not isinstance(message, str):
            raise TypeError("`message` must be a string.")

        self._msg = message
        self._msg_bytes = self._msg.encode("ASCII")
        self._db.put(_MSG_KEY, self._msg_bytes)

    def set_start_n_info(self, head, tail_length):
        """Set the range of the `start_n` parameters of disk `Block`s belonging to this `Register`.

        If the `start_n` parameter is very large (of order more than trillions), then the `Register` database can
        become very bloated by storing many redundant digits for the `start_n` parameter. Calling this method with
        appropriate `head` and `tail_length` parameters alleviates the bloat.

        The "head" and "tail" of a non-negative number x is defined by x = head * 10^L + tail, where L is the "length",
        or the number of digits, of "tail". (L must be at least 1, and 0 is considered to have 1 digit.)

        By calling `set_start_n_info(head, tail_length)`, the user is asserting that the start_n of every disk
        `Block` belong to this `Register` can be decomposed in the fashion start_n = head * 10^tail_length + tail. The
        user is discouraged to call this method for large `tail_length` values (>12), as this is likely unnecessary and
        defeats the purpose of this method.

        :param head: (type `int`) non-negative
        :param tail_length: (type `int`) positive
        """

        self._check_open_raise("set_start_n_info")

        if not is_int(head):
            raise TypeError("`head` must be of type `int`.")
        else:
            head = int(head)

        if not is_int(tail_length):
            raise TypeError("`tail_length` must of of type `int`.")
        else:
            tail_length = int(tail_length)

        if head < 0:
            raise ValueError("`head` must be non-negative.")

        if tail_length <= 0:
            raise ValueError("`tail_length` must be positive.")

        if head == self._start_n_head and tail_length == self._start_n_tail_length:
            return

        failure_reinserts = []

        new_mod = 10 ** tail_length
        with leveldb_prefix_iterator(self._db, _BLK_KEY_PREFIX) as it:
            for key, _ in it:
                apri, start_n, length = self._convert_disk_block_key(_BLK_KEY_PREFIX_LEN, key)
                if start_n // new_mod != head:
                    raise ValueError(
                        "The following `start_n` does not have the correct head:\n" +
                        f"`start_n`   : {start_n}\n" +
                        "That `start_n` is associated with a `Block` whose `Apri_Info` and length is:\n" +
                        f"`Apri_Info` : {str(apri.to_json())}\n" +
                        f"length      : {length}"
                    )

        try:

            with self._db.write_batch(transaction=True) as wb:

                wb.put(_START_N_HEAD_KEY, str(head).encode("ASCII"))
                wb.put(_START_N_TAIL_LENGTH_KEY, str(tail_length).encode("ASCII"))

                with self._db.snapshot() as sn:
                    with leveldb_prefix_iterator(sn, _BLK_KEY_PREFIX) as it:
                        for key, val in it:

                            _, start_n, _ = self._convert_disk_block_key(_BLK_KEY_PREFIX_LEN, key)
                            apri_json, _, length_bytes = Register._split_disk_block_key(_BLK_KEY_PREFIX_LEN, key)

                            new_start_n_bytes = str(start_n % new_mod).encode("ASCII")

                            new_key = Register._join_disk_block_data(
                                _BLK_KEY_PREFIX, apri_json, new_start_n_bytes, length_bytes
                            )

                            if key != new_key:
                                wb.put(new_key, val)
                                self._db.delete(key)
                                failure_reinserts.append((key, val))

        except RuntimeError as e:

            with self._db.write_batch(transaction = True) as wb:
                for key, val in failure_reinserts:
                    wb.put(key, val)

            raise e

        self._start_n_head = head
        self._start_n_tail_length = tail_length
        self._start_n_tail_mod = 10 ** self._start_n_tail_length

    @contextmanager
    def open(self):

        if not self._created:
            # set local directory info and create levelDB database
            local_dir = random_unique_filename(self.saves_directory, length=4, alphabet=LOCAL_DIR_CHARS)
            local_dir.mkdir()
            self._reg_file = local_dir / REGISTER_LEVELDB_FILENAME
            self._version_file = local_dir / VERSION_FILE_NAME
            self._db = plyvel.DB(str(self._reg_file), create_if_missing= True)
            self._set_local_dir(local_dir)

            with self._db.write_batch(transaction = True) as wb:
                # set register info
                wb.put(_CLS_KEY, self._reg_cls_bytes)
                wb.put(_MSG_KEY, self._msg_bytes)
                wb.put(_START_N_HEAD_KEY, str(self._start_n_head).encode("ASCII"))
                wb.put(_START_N_TAIL_LENGTH_KEY, str(self._start_n_tail_length).encode("ASCII"))
                wb.put(_CURR_ID_KEY, b"0")

            with self._version_file.open("w") as fh:
                fh.write(CURRENT_VERSION)

            Register._add_instance(local_dir, self)
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

        if Register._instance_exists(self._local_dir):
            ret = Register._get_instance(self._local_dir)
        else:
            ret = self

        if not ret._created:
            raise Register_Error(Register._NOT_CREATED_ERROR_MESSAGE.format("_open_created"))

        if ret._db is not None and not ret._db.closed:
            raise Register_Already_Open_Error()

        ret._db = plyvel.DB(str(ret._reg_file))

        return ret

    def _close_created(self):
        self._db.close()

    @contextmanager
    def _recursive_open(self):
        if not self._created:
            raise Register_Error(Register._NOT_CREATED_ERROR_MESSAGE.format("_recursive_open"))
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
            raise Register_Error(
                f"The `Register` database has not been opened. You must open this register via `with reg.open() as " +
                f"reg:` before calling the method `{method_name}`."
            )

        if not self._local_dir.exists() or not self._local_dir.is_dir():
            raise FileNotFoundError(Register._LOCAL_DIR_ERROR_MSG)

    def _set_local_dir(self, local_dir):
        """
        :param local_dir: (type `pathlib.Path`)
        """

        if local_dir.parent.resolve() != self.saves_directory.resolve():
            raise ValueError(
                "The `local_dir` argument must be a sub-directory of `reg.saves_directory`.\n" +
                f"`local_dir.parent`    : {str(local_dir.parent)}\n"
                f"`reg.saves_directory` : {str(self.saves_directory)}"
            )
        if not local_dir.is_dir():
            raise FileNotFoundError(Register._LOCAL_DIR_ERROR_MSG)
        if not (local_dir / REGISTER_LEVELDB_FILENAME).is_dir():
           raise FileNotFoundError(Register._LOCAL_DIR_ERROR_MSG)
        self._created = True
        self._local_dir = local_dir
        self._local_dir_bytes = str(self._local_dir).encode("ASCII")
        self._reg_file = self._local_dir / REGISTER_LEVELDB_FILENAME
        self._subreg_bytes = (
            _SUB_KEY_PREFIX + self._local_dir_bytes
        )

    @staticmethod
    def _is_compatible_version(local_dir):

        with (local_dir / REGISTER_LEVELDB_FILENAME / VERSION_FILE_NAME).open("r") as fh:
            return fh.readline().trim() in COMPATIBLE_VERSIONS

    #################################
    #      PUBLIC APRI METHODS      #

    def get_all_apri_info(self, recursively = False):

        if not isinstance(recursively, bool):
            raise TypeError("`recursively` must be of type `bool`")

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

    def change_apri_info(self, old_apri, new_apri, recursively = False):

        self._check_open_raise("change_apri_info")

        failure_reinserts = []

        old_apri_id = self._get_id_by_apri(old_apri, None, False)
        old_apri_json = b"Apri_Info.from_json(" + old_apri.to_json().encode("ASCII") + b")"
        old_apri_key = _APRI_ID_KEY_PREFIX + old_apri_json
        old_apri_id_key = _ID_APRI_KEY_PREFIX + old_apri_id
        old_apri_apos_key = _APOS_KEY_PREFIX + old_apri_id

        new_apri_json = b"Apri_Info.from_json(" + new_apri.to_json().encode("ASCII") + b")"

        try:

            self._db.delete(old_apri_key)
            failure_reinserts.append((old_apri_key, old_apri_id))
            self._db.delete(old_apri_id_key)
            failure_reinserts.append((old_apri_id_key, old_apri_json))

            new_apri_id = self._get_id_by_apri(new_apri, new_apri_json, True)

            with self._db.write_batch(transaction = True) as wb:
                with self._db.snapshot() as sn:

                    # change all id_apri keys
                    with leveldb_prefix_iterator(sn, _ID_APRI_KEY_PREFIX) as it:
                        for key, val in it:

                            new_val = val.replace(old_apri_json, new_apri_json)

                            if val != new_val:
                                wb.put(key, new_val)

                    # change all apri_id keys
                    with leveldb_prefix_iterator(sn, _APRI_ID_KEY_PREFIX) as it:
                        for key, val in it:

                            changed_key = key.replace(old_apri_json, new_apri_json)

                            if changed_key != key:

                                wb.put(changed_key, val)
                                self._db.delete(key)
                                failure_reinserts.append((key, val))

                    # change all uncompressed blocks
                    with leveldb_prefix_iterator(sn, _BLK_KEY_PREFIX + old_apri_id + _KEY_SEP) as it:
                        for key, val in it:

                            new_blk_key = _BLK_KEY_PREFIX + new_apri_id + key[key.index(_KEY_SEP) : ]
                            wb.put(new_blk_key, val)

                    # change all compressed blocks
                    with leveldb_prefix_iterator(sn, _COMPRESSED_KEY_PREFIX + old_apri_id + _KEY_SEP) as it:
                        for key, val in it:

                            new_compr_key = _COMPRESSED_KEY_PREFIX + new_apri_id + key[key.index(_KEY_SEP) : ]
                            wb.put(new_compr_key, val)

                    # change all apos vals
                    with leveldb_prefix_iterator(sn, _APOS_KEY_PREFIX) as it:
                        for key, val in it:

                            new_val = val.replace(old_apri_json, new_apri_json)

                            if key == old_apri_apos_key:

                                new_key = _APOS_KEY_PREFIX + new_apri_id
                                self._db.delete(old_apri_apos_key)
                                failure_reinserts.append((old_apri_apos_key, val))

                            else:
                                new_key = key

                            if key != new_key or val != new_val:
                                wb.put(new_key, new_val)


        except RuntimeError as e:

            with self._db.write_batch(transaction = True) as wb:
                for key, val in failure_reinserts:
                    wb.put(key, val)

            raise e

        if recursively:
            for subreg in self._iter_subregisters():
                with subreg._recursive_open() as subreg:
                    subreg.change_apri_info(old_apri, new_apri, True)

    #################################
    #      PROTEC APRI METHODS      #

    def _get_apri_json_by_id(self, _id, db = None):
        """Get JSON bytestring representing an `Apri_Info` instance.

        :param _id: (type `bytes`)
        :param db: (type `plyvel.DB`, default `None`) The database to query. If `None`, then use `self._db`.
        (Intended for database snapshots.)
        :return: (type `bytes`)
        """

        if db is None:
            db = self._db

        return db.get(_ID_APRI_KEY_PREFIX + _id)

    def _get_id_by_apri(self, apri, apri_json, missing_ok, db = None):
        """Get an `Apri_Info` ID for this database. If `missing_ok is True`, then create an ID if the passed `apri` or
        `apri_json` is unknown to this `Register`.

        One of `apri` and `apri_json` can be `None`, but not both. If both are not `None`, then `apri` is used.

        `self._db` must be opened by the caller.

        :param apri: (type `Apri_Info`)
        :param apri_json: (type `bytes`)
        :param missing_ok: (type `bool`) Create an ID if the passed `apri` or `apri_json` is unknown to this `Register`.
        :param db: (type `plyvel.DB`, default `None`) The database to query. If `None`, then use `self._db`.
        (Intended for database snapshots.)
        :raises Apri_Info_Not_Found_Error: If `apri` or `apri_json` is not known to this `Register` and `missing_ok
        is False`.
        :return: (type `bytes`)
        """

        if apri is not None:
            key = _APRI_ID_KEY_PREFIX + apri.to_json().encode("ASCII")
        elif apri_json is not None:
            key = _APRI_ID_KEY_PREFIX + apri_json
        else:
            raise ValueError

        if db is None:
            db = self._db

        _id = db.get(key, default = None)

        if _id is not None:
            return _id

        elif missing_ok:
            _id = db.get(_CURR_ID_KEY)
            next_id = str(int(_id) + 1).encode("ASCII")
            with db.write_batch(transaction = True) as wb:
                wb.put(_CURR_ID_KEY, next_id)
                wb.put(key, _id)
                wb.put(_ID_APRI_KEY_PREFIX + _id, key[_APRI_ID_KEY_PREFIX_LEN:])
            return _id

        else:
            if apri is None:
                apri = Apri_Info.from_json(apri_json.decode("ASCII"))
            raise Data_Not_Found_Error(f"`{str(apri)}` is not known to this `Register`.")

    #################################
    #      PUBLIC APOS METHODS      #

    def set_apos_info(self, apri, apos):
        """Set some `Apos_Info` for corresponding `Apri_Info`.

        WARNING: This method will OVERWRITE any previous saved `Apos_Info`. If you do not want to lose any previously
        saved data, then you should do something like the following:

            apos = reg.get_apos_info(apri)
            apos.period_length = 5
            reg.set_apos_info(apos)

        :param apri: (type `Apri_Info`)
        :param apos: (type `Apos_Info`)
        """

        self._check_open_raise("set_apos_info")

        if not isinstance(apri, Apri_Info):
            raise TypeError("`apri` must be of type `Apri_Info`")

        if not isinstance(apos, Apos_Info):
            raise TypeError("`apos` must be of type `Apos_Info`")

        key = self._get_apos_key(apri, None, True)
        apos_json = apos.to_json().encode("ASCII")
        self._db.put(key, apos_json)

    def get_apos_info(self, apri):
        """Get some `Apos_Info` associated with a given `Apri_Info`.

        :param apri: (type `Apri_Info`)
        :raises Apri_Info_Not_Found_Error: If `apri` is not known to this `Register`.
        :raises Data_Not_Found_Error: If no `Apos_Info` has been associated to `apri`.
        :return: (type `Apos_Info`)
        """

        self._check_open_raise("get_apos_info")

        if not isinstance(apri, Apri_Info):
            raise TypeError("`apri` must be of type `Apri_Info`")

        key = self._get_apos_key(apri, None, False)
        apos_json = self._db.get(key, default=None)

        if apos_json is not None:
            return Apos_Info.from_json(apos_json.decode("ASCII"))

        else:
            raise Data_Not_Found_Error(f"No `Apos_Info` associated with `{str(apri)}`.")

    def remove_apos_info(self, apri, apos):

        self._check_open_raise("remove_apos_info")

        if not isinstance(apri, Apri_Info):
            raise TypeError("`apri` must be of type `Apri_Info`.")

        if not isinstance(apos, Apos_Info):
            raise TypeError("`apos` must be of type `Apos_Info`.")

        key = self._get_apos_key(apri, None, False)

        if leveldb_has_key(self._db, key):
            self._db.delete(key)

        else:
            raise Data_Not_Found_Error(f"No `Apos_Info` associated with `{str(apri)}`.")

    #################################
    #      PROTEC APOS METHODS      #

    def _get_apos_key(self, apri, apri_json, missing_ok, db = None):
        """Get a key for an `Apos_Info` entry.

        One of `apri` and `apri_json` can be `None`, but not both. If both are not `None`, then `apri` is used. If
        `missing_ok is True`, then create a new `Apri_Info` ID if one does not already exist for `apri`.

        :param apri: (type `Apri_Info`)
        :param apri_json: (type `bytes`)
        :param missing_ok: (type `bool`)
        :param db: (type `plyvel.DB`, default `None`) The database to query. If `None`, then use `self._db`.
        (Intended for database snapshots.)
        :raises Apri_Info_Not_Found_Error: If `missing_ok is False` and `apri` is not known to this `Register`.
        :return: (type `bytes`)
        """

        if apri is None and apri_json is None:
            raise ValueError

        apri_id = self._get_id_by_apri(apri, apri_json, missing_ok, db)
        return _APOS_KEY_PREFIX + _KEY_SEP + apri_id

    #################################
    #  PUBLIC SUB-REGISTER METHODS  #

    def add_subregister(self, subreg):

        self._check_open_raise("add_subregister")

        if not isinstance(subreg, Register):
            raise TypeError("`subreg` must be of a `Register` derived type")

        if not subreg._created:
            raise Register_Error(Register._NOT_CREATED_ERROR_MESSAGE.format("add_subregister"))

        key = subreg._get_subregister_key()
        if not leveldb_has_key(self._db, key):
            if subreg._check_no_cycles(self):
                self._db.put(key, subreg._reg_cls_bytes)
            else:
                raise Register_Error(
                    "Attempting to add this register as a sub-register will created a directed cycle in the " +
                    "subregister relation. \n" +
                    f"Description of the intended super-register:\n\"{str(self)}\"\n" +
                    f"Description of the intended sub-register:\n\"{str(subreg)}\""
                )

    def remove_subregister(self, subreg):

        self._check_open_raise("remove_subregister")

        if not isinstance(subreg, Register):
            raise TypeError("`subreg` must be of a `Register` derived type")

        key = subreg._get_subregister_key()
        if leveldb_has_key(self._db, key):
            self._db.delete(key)

    #################################
    #  PROTEC SUB-REGISTER METHODS  #

    def _check_no_cycles(self, original):

        if not self._created or not original._created:
            raise Register_Error(Register._NOT_CREATED_ERROR_MESSAGE.format("_check_no_cycles"))

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
                filename = Path(key[length:].decode("ASCII"))
                subreg = Register._from_local_dir(filename)
                yield subreg

    def _get_subregister_key(self):
        return _SUB_KEY_PREFIX + self._local_dir_bytes

    #################################
    #    PUBLIC DISK BLK METHODS    #

    @classmethod
    @abstractmethod
    def dump_disk_data(cls, data, filename, **kwargs):
        """Dump data to the disk.

        This method should not change any properties of any `Register`, which is why it is a class-method and
        not an instance-method. It merely takes `data` and dumps it to disk.

        Most use-cases prefer the instance-method `add_disk_block`.

        :param data: (any type) The raw data to dump.
        :param filename: (type `pathlib.Path`) The filename to dump to. You may edit this filename if
        necessary (such as by adding a suffix), but you must return the edited filename.
        :raises OSError: If the dump fails for any reason.
        :return: (type `pathlib.Path`) The actual filename of the data on the disk.
        """

    @classmethod
    @abstractmethod
    def load_disk_data(cls, filename, **kwargs):
        """Load raw data from the disk.

        This method should not change any properties of any `Register`, which is why it is a classmethod and
        not an instancemethod. It merely loads the raw data saved on the disk and returns it.

        Most use-cases prefer the method `get_disk_block`.

        :param filename: (type `pathlib.Path`) Where to load the block from. You may need to edit this
        filename if necessary, such as by adding a suffix, but you must return the edited filename.
        :raises Data_Not_Found_Error: If the data could not be loaded because it doesn't exist.
        :raises OSError: If the data exists but couldn't be loaded for any reason.
        :return: (any type) The data loaded from the disk.
        :return: (pathlib.Path) The exact path of the data saved to the disk.
        """

    def add_disk_block(self, blk, return_metadata = False, **kwargs):
        """Dump a `Block` to disk and link it with this `Register`.

        :param blk: (type `Block`)
        :param return_metadata: (type `bool`, default `False`) Whether to return a `File_Metadata` object, which
        contains file creation date/time and size of dumped data to the disk.
        :raises Register_Error: If a duplicate `Block` already exists in this `Register`.
        """

        self._check_open_raise("add_disk_block")

        if not isinstance(blk, Block):
            raise TypeError("`blk` must be of type `Block`.")

        if not isinstance(return_metadata, bool):
            raise TypeError("`return_metadata` must be of type `bool`.")

        start_n_head = blk.get_start_n() // self._start_n_tail_mod
        if start_n_head != self._start_n_head :
            raise IndexError(
                "The `start_n` for the passed `Block` does not have the correct head:\n" +
                f"`tail_length`   : {self._start_n_tail_length}\n" +
                f"expected `head` : {self._start_n_head}\n"
                f"`start_n`       : {blk.get_start_n()}\n" +
                f"`start_n` head  : {start_n_head}\n"
            )

        created_apri = blk.get_apri() not in self

        # this will create an apri_id if necessary, but it does not put `blk_key` into the database
        blk_key = self._get_disk_block_key(_BLK_KEY_PREFIX, blk.get_apri(), None, blk.get_start_n(), len(blk), True)

        try:

            if not leveldb_has_key(self._db, blk_key):

                filename = random_unique_filename(self._local_dir, length=6)
                filename = type(self).dump_disk_data(blk.get_segment(), filename, **kwargs)

                filename_bytes = str(filename.name).encode("ASCII")
                compressed_key = _COMPRESSED_KEY_PREFIX + blk_key[_BLK_KEY_PREFIX_LEN : ]

                try:
                    with self._db.write_batch(transaction = True) as wb:

                        wb.put(blk_key, filename_bytes)
                        wb.put(compressed_key, _IS_NOT_COMPRESSED_VAL)

                except RuntimeError as e:

                    filename.unlink(missing_ok = True)
                    raise e

                if len(blk) == 0:
                    warnings.warn(
                        "Added a length 0 disk block to a `Register`.\n" +
                        f"`Register` message: {str(self)}\n" +
                        f"`Block`: {str(blk)}" +
                        f"`Register` location: {str(self._local_dir)}\n"
                    )

                if return_metadata:
                    return File_Metadata.from_path(filename)

            else:
                raise Register_Error(
                    f"Duplicate `Block` with the following data already exists in this `Register`: " +
                    f"{str(blk.get_apri())}, start_n = {blk.get_start_n()}, length = {len(blk)}."
                )

        except RuntimeError as e:

            if created_apri:

                apri_key = _APRI_ID_KEY_PREFIX + blk.get_apri().to_json().encode("ASCII")
                apri_id = self._db.get(apri_key)
                id_apri_key = _ID_APRI_KEY_PREFIX + apri_id

                self._db.delete(apri_key)
                self._db.delete(id_apri_key)

            raise e

    def remove_disk_block(self, apri, start_n, length, recursively = False):

        self._check_open_raise("remove_disk_block")

        start_n, length = Register._check_apri_start_n_length_raise(apri, start_n, length)

        try:
            blk_key, compressed_key = self._check_blk_compressed_keys_raise(None, None, apri, None, start_n, length)

        except Data_Not_Found_Error:
            pass

        else:
            blk_filename, compressed_filename = self._check_blk_compressed_files_raise(
                blk_key, compressed_key, apri, start_n, length
            )



            if compressed_filename is not None:
                compressed_filename.unlink(missing_ok=False)

            blk_filename.unlink(missing_ok=False)

            self._db.delete(compressed_key)
            self._db.delete(blk_key)

            return

        if recursively:

            for subreg in self._iter_subregisters():
                with subreg._recursive_open() as subreg:
                    try:
                        subreg.remove_disk_block(apri, start_n, length, True)

                    except Data_Not_Found_Error:
                        pass

                    else:
                        return

        raise Data_Not_Found_Error(
            Register._DISK_BLOCK_DATA_NOT_FOUND_ERROR_MSG_FULL.format(str(apri), start_n, length)
        )
                    
    def remove_all_disk_blocks(self, apri, recursively = False):

        self._check_open_raise("remove_all_disk_blocks")

        for start_n, length in self.get_disk_block_intervals(apri):
            self.remove_disk_block(apri, start_n, length, recursively)

    def get_disk_block(self, apri, start_n, length, return_metadata = False, recursively = False, **kwargs):

        self._check_open_raise("get_disk_block")

        start_n, length = Register._check_apri_start_n_length_raise(apri, start_n, length)

        if not isinstance(return_metadata, bool):
            raise TypeError("`return_metadata` must be of type `bool`.")

        if not isinstance(recursively, bool):
            raise TypeError("`recursively` must be of type `bool`.")

        try:
            blk_key, compressed_key = self._check_blk_compressed_keys_raise(None, None, apri, None, start_n, length)

        except Data_Not_Found_Error:
            pass

        else:

            if self._db.get(compressed_key) != _IS_NOT_COMPRESSED_VAL:
                raise Compression_Error(
                    "Could not load `Block` with the following data because the `Block` is compressed. Please call " +
                    "the `Register` method `decompress` first before loading the data.\n" +
                    f"{apri}, start_n = {start_n}, length = {length}"
                )

            blk_filename, _ = self._check_blk_compressed_files_raise(blk_key, compressed_key, apri, start_n, length)
            blk_filename = self._local_dir / blk_filename
            data, blk_filename = type(self).load_disk_data(blk_filename, **kwargs)
            blk = Block(data, apri, start_n)

            if return_metadata:
                return blk, File_Metadata.from_path(blk_filename)

            else:
                return blk

        if recursively:
            for subreg in self._iter_subregisters():
                with subreg._recursive_open() as subreg:
                    try:
                        return subreg.get_disk_block(apri, start_n, length, return_metadata, True)

                    except Data_Not_Found_Error:
                        pass

        raise Data_Not_Found_Error(
            Register._DISK_BLOCK_DATA_NOT_FOUND_ERROR_MSG_FULL.format(str(apri), start_n, length)
        )

    def get_disk_block_by_n(self, apri, n, return_metadata = False, recursively = False):

        self._check_open_raise("get_disk_block_by_n")

        if not isinstance(apri, Apri_Info):
            raise TypeError("`apri` must be of type `Apri_Info`.")

        if not is_int(n):
            raise TypeError("`n` must be of type `int`.")
        else:
            n = int(n)

        if not isinstance(return_metadata, bool):
            raise TypeError("`return_metadata` must be of type `bool`.")

        if not isinstance(recursively, bool):
            raise TypeError("`recursively` must be of type `bool`.")

        if n < 0:
            raise ValueError("`n` must be non-negative")

        try:
            for start_n, length in self.get_disk_block_intervals(apri):
                if start_n <= n < start_n + length:
                    return self.get_disk_block(apri, start_n, length, return_metadata, False)

        except Data_Not_Found_Error:
            pass

        if recursively:
            for subreg in self._iter_subregisters():
                with subreg._recursive_open() as subreg:
                    try:
                        return subreg.get_disk_block_by_n(apri, n, return_metadata, True)

                    except Data_Not_Found_Error:
                        pass

        raise Data_Not_Found_Error(Register._DISK_BLOCK_DATA_NOT_FOUND_ERROR_MSG_N.format(str(apri), n))

    def get_all_disk_blocks(self, apri, return_metadata = False, recursively = False):

        self._check_open_raise("get_all_disk_blocks")

        if not isinstance(apri, Apri_Info):
            raise TypeError("`apri` must be of type `Apri_Info`.")

        if not isinstance(return_metadata, bool):
            raise TypeError("`return_metadata` must be of type `bool`.")

        if not isinstance(recursively, bool):
            raise TypeError("`recursively` must be of type `bool`.")


        for start_n, length in self.get_disk_block_intervals(apri):
            try:
                yield self.get_disk_block(apri, start_n, length, return_metadata, False)

            except Data_Not_Found_Error:
                pass

        if recursively:
            for subreg in self._iter_subregisters():
                with subreg._recursive_open() as subreg:
                    for blk in subreg.get_all_disk_blocks(apri, return_metadata, True):
                        yield blk

    def get_disk_block_metadata(self, apri, start_n, length, recursively = False):

        self._check_open_raise("get_disk_block_metadata")

        start_n, length = Register._check_apri_start_n_length_raise(apri, start_n, length)

        if not isinstance(recursively, bool):
            raise TypeError("`recursively` must be of type `bool`.")

        try:
            blk_key, compressed_key = self._check_blk_compressed_keys_raise(None, None, apri, None, start_n, length)

        except Data_Not_Found_Error:
            pass

        else:
            blk_filename, compressed_filename = self._check_blk_compressed_files_raise(
                blk_key, compressed_key, apri, start_n, length
            )

            if compressed_filename is not None:
                return File_Metadata.from_path(compressed_filename)

            else:
                return File_Metadata.from_path(blk_filename)

        if recursively:
            for subreg in self._iter_subregisters():
                with subreg._recursive_open() as subreg:
                    try:
                        return subreg.get_disk_block_metadata(apri, start_n, length, True)

                    except Data_Not_Found_Error:
                        pass

        raise Data_Not_Found_Error(
            Register._DISK_BLOCK_DATA_NOT_FOUND_ERROR_MSG_FULL.format(str(apri), start_n, length)
        )

    def get_disk_block_intervals(self, apri):
        """Return a `list` of all tuples `(start_n, length)` associated to disk `Block`s.

        The tuples are sorted by increasing `start_n` and the larger `length` is used to break ties.

        :param apri: (type `Apri_Info`)
        :return: (type `list`)
        """

        if not isinstance(apri, Apri_Info):
            raise TypeError("`apri` must be of type `Apri_Info`.")

        return sorted([
            self._convert_disk_block_key(_BLK_KEY_PREFIX_LEN, key, apri)[1:]
            for key, _ in self._iter_disk_block_pairs(_BLK_KEY_PREFIX, apri, None)
        ], key = lambda t: (t[0], -t[1]))

    def compress(self, apri, start_n, length, compression_level = 6, return_metadata = False):
        """Compress a `Block`.

        :param apri: (type `Apri_Info`)
        :param start_n: (type `int`) Non-negative.
        :param length: (type `int`) Non-negative.
        :param compression_level: (type `int`, default 6) Between 0 and 9, inclusive. 0 is for the fastest compression,
        but lowest compression ratio; 9 is slowest, but highest ratio. See
        https://docs.python.org/3/library/zlib.html#zlib.compressobj for more information.
        :param return_metadata: (type `bool`, default `False`) Whether to return a `File_Metadata` object that
        describes the compressed file.
        :raises Compression_Error: If the `Block` is already compressed.
        :return: (type `File_Metadata`) If `return_metadata is True`.
        """

        self._check_open_raise("compress")

        start_n, length = Register._check_apri_start_n_length_raise(apri, start_n, length)

        if not is_int(compression_level):
            raise TypeError("`compression_level` must be of type `int`.")
        else:
            compression_level = int(compression_level)

        if not isinstance(return_metadata, bool):
            raise TypeError("`return_metadata` must be of type `bool`.")

        if not (0 <= compression_level <= 9):
            raise ValueError("`compression_level` must be between 0 and 9.")

        compressed_key = self._get_disk_block_key(
            _COMPRESSED_KEY_PREFIX, apri, None, start_n, length, False
        )

        blk_filename = self._compress_helper_check_keys(compressed_key, apri, start_n, length)

        compressed_fh = None

        try:
            compressed_val, compressed_filename, compressed_fh = self._compress_helper_open_zipfile(compression_level)
            Register._compress_helper_write_data(compressed_fh, blk_filename)

        finally:
            if compressed_fh is not None:
                compressed_fh.close()

        self._compress_helper_update_key(compressed_key, compressed_val)
        Register._compress_helper_clean_uncompressed_data(compressed_filename, blk_filename)

        if return_metadata:
            return File_Metadata.from_path(compressed_filename)

        else:
            return None

    def compress_all(self, apri, compression_level = 6, return_metadata = False):
        """Compress all non-compressed `Block`s. Any `Block`s that are already compressed will be skipped.

        :param apri: (type `Apri_Info`)
        :param compression_level: (type `int`, default 6) Between 0 and 9, inclusive. 0 is for the fastest compression,
        but lowest compression ratio; 9 is slowest, but highest ratio. See
        https://docs.python.org/3/library/zlib.html#zlib.compressobj for more information.
        :param return_metadata: (type `bool`, default `False`) Whether to return a `File_Metadata` object that
        describes the compressed file.
        :return: (type `File_Metadata`) If `return_metadata is True`.
        """

        self._check_open_raise("compress_all")

        if not isinstance(apri, Apri_Info):
            raise TypeError("`apri` must be of type `Apri_Info`.")

        if not is_int(compression_level):
            raise TypeError("`compression_level` must be of type `int`.")
        else:
            compression_level = int(compression_level)

        if not isinstance(return_metadata, bool):
            raise TypeError("`return_metadata` must be of type `bool`.")

        if not (0 <= compression_level <= 9):
            raise ValueError("`compression_level` must be between 0 and 9.")

        compressed_fh = None
        to_clean = []

        try:
            compressed_val, compressed_filename, compressed_fh = self._compress_helper_open_zipfile(compression_level)

            for blk_key, _ in self._iter_disk_block_pairs(_BLK_KEY_PREFIX, apri, None):

                compressed_key = _COMPRESSED_KEY_PREFIX + blk_key[_BLK_KEY_PREFIX_LEN : ]
                apri, start_n, length = self._convert_disk_block_key(_BLK_KEY_PREFIX_LEN, blk_key, apri)

                try:
                    blk_filename = self._compress_helper_check_keys(compressed_key, apri, start_n, length)

                except Compression_Error:
                    pass

                else:
                    Register._compress_helper_write_data(compressed_fh, blk_filename)
                    self._compress_helper_update_key(compressed_key, compressed_val)
                    to_clean.append(blk_filename)

        finally:
            if compressed_fh is not None:
                compressed_fh.close()

        for blk_filename in to_clean:
            Register._compress_helper_clean_uncompressed_data(compressed_filename, blk_filename)

        if return_metadata:
            return File_Metadata.from_path(compressed_filename)

        else:
            return None

    def decompress(self, apri, start_n, length, return_metadata = False):
        """Decompress a `Block`.

        :param apri: (type `Apri_Info`)
        :param start_n: (type `int`) Non-negative.
        :param length: (type `int`) Non-negative.
        :param return_metadata: (type `bool`, default `False`) Whether to return a `File_Metadata` object that
        describes the decompressed file.
        :raise Decompression_Error: If the `Block` is not compressed.
        :return: (type `list`) If `return_metadata is True`.
        """

        self._check_open_raise("decompress")

        start_n, length = Register._check_apri_start_n_length_raise(apri, start_n, length)

        if not isinstance(return_metadata, bool):
            raise TypeError("`return_metadata` must be of type `bool`.")

        compressed_filename, blk_filename = self._decompress_helper(apri, start_n, length)

        if zip_archive_is_empty(compressed_filename):
            compressed_filename.unlink(missing_ok = False)

        if return_metadata:
            return File_Metadata.from_path(blk_filename)

        else:
            return None

    def decompress_all(self, apri, return_metadatas = False):
        """Decompress all compressed `Block`s. Any `Block`s that are not compressed will be skipped.

        :param apri: (type `Apri_Info`)
        :param return_metadatas: (type `bool`, default `False`) Whether to return a `list` of `File_Metadata` objects
        that describes the decompressed file(s).
        :return: (type `list`) If `return_metadatas is True`.
        """

        self._check_open_raise("decompress_all")

        if not isinstance(apri, Apri_Info):
            raise TypeError("`apri` must be of type `Apri_Info`.")

        if not isinstance(return_metadatas, bool):
            raise TypeError("`return_metadatas` must be of type `bool`.")

        compressed_filenames = []
        blk_filenames = []

        for start_n, length in self.get_disk_block_intervals(apri):

            try:
                compressed_filename, blk_filename = self._decompress_helper(apri, start_n, length)

            except Decompression_Error:
                pass

            else:
                compressed_filenames.append(compressed_filename)
                blk_filenames.append(blk_filename)

        for compressed_filename in compressed_filenames:
            if zip_archive_is_empty(compressed_filename):
                compressed_filename.unlink(missing_ok = False)

        if return_metadatas:
            return [File_Metadata.from_path(blk_filename) for blk_filename in blk_filenames]

        else:
            return None

    #################################
    #    PROTEC DISK BLK METHODS    #

    def _get_disk_block_key(self, prefix, apri, apri_json, start_n, length, missing_ok, db = None):
        """Get the database key for a disk `Block`.

        One of `apri` and `apri_json` can be `None`, but not both. If both are not `None`, then `apri` is used.
        `self._db` must be opened by the caller. This method only queries the database to obtain the `apri` ID.

        If `missing_ok is True` and an ID for `apri` does not already exist, then a new one will be created. If
        `missing_ok is False` and an ID does not already exist, then an error is raised.

        :param prefix: (type `bytes`)
        :param apri: (type `Apri_Info`)
        :param apri_json: (types `bytes`)
        :param start_n: (type `int`) The start index of the `Block`.
        :param length: (type `int`) The length of the `Block`.
        :param missing_ok: (type `bool`)
        :param db: (type `plyvel.DB`, default `None`) The database to query. If `None`, then use `self._db`.
        (Intended for database snapshots.)
        :raises Apri_Info_Not_Found_Error: If `missing_ok is False` and `apri` is not known to this `Register`.
        :return: (type `bytes`)
        """

        if apri is None and apri_json is None:
            raise ValueError

        _id = self._get_id_by_apri(apri, apri_json, missing_ok, db)
        tail = start_n % self._start_n_tail_mod

        return (
                prefix                      +
                _id                         + _KEY_SEP +
                str(tail)  .encode("ASCII") + _KEY_SEP +
                str(length).encode("ASCII")
        )

    def _iter_disk_block_pairs(self, prefix, apri, apri_json, db = None):
        """Iterate over key-value pairs for block entries.

        :param prefix: (type `bytes`)
        :param apri: (type `Apri_Info`)
        :param apri_json: (type `bytes`)
        :param db: (type `plyvel.DB`, default `None`) The database to query. If `None`, then use `self._db`.
        (Intended for database snapshots.)
        :return: (type `bytes`) key
        :return: (type `bytes`) val
        """

        if apri_json is not None or apri is not None:
            prefix += self._get_id_by_apri(apri,apri_json,False,db)
            prefix += _KEY_SEP

        with self._db.snapshot() as sn:
            with leveldb_prefix_iterator(sn, prefix) as it:
                for key,val in it:
                    yield key, val

    @staticmethod
    def _split_disk_block_key(prefix_len, key):
        return tuple(key[prefix_len:].split(_KEY_SEP))

    @staticmethod
    def _join_disk_block_data(prefix, apri_json, start_n_bytes, length_bytes):
        return (
            prefix +
            apri_json       + _KEY_SEP +
            start_n_bytes   + _KEY_SEP +
            length_bytes
        )

    def _convert_disk_block_key(self, prefix_len, key, apri = None, db = None):
        """
        :param prefix_len: (type `int`) Positive.
        :param key: (type `bytes`)
        :param apri: (type `Apri_Info`, default None) If `None`, the relevant `apri` is acquired through a database
        query.
        :param db: (type `plyvel.DB`, default `None`) The database to query. If `None`, then use `self._db`.
        (Intended for database snapshots.)
        :return: (type `Apri_Info`)
        :return (type `int`) start_n
        :return (type `int`) length, non-negative
        """

        apri_id, start_n_bytes, length_bytes = Register._split_disk_block_key(prefix_len, key)

        if apri is None:
            apri_json = self._get_apri_json_by_id(apri_id, db)
            apri = Apri_Info.from_json(apri_json.decode("ASCII"))

        return (
            apri,
            int(start_n_bytes.decode("ASCII")) + self._start_n_head * self._start_n_tail_mod,
            int(length_bytes.decode("ASCII"))
        )

    def _check_blk_compressed_keys_raise(self, blk_key, compressed_key, apri, apri_json, start_n, length):

        if compressed_key is None and blk_key is None:
            compressed_key = self._get_disk_block_key(_COMPRESSED_KEY_PREFIX, apri, apri_json, start_n, length, False)

        if blk_key is not None and compressed_key is None:
            compressed_key = _COMPRESSED_KEY_PREFIX + blk_key[_BLK_KEY_PREFIX_LEN : ]

        elif compressed_key is not None and blk_key is None:
            blk_key = _BLK_KEY_PREFIX + compressed_key[_COMPRESSED_KEY_PREFIX_LEN : ]

        if apri is None:
            apri = Apri_Info.from_json(apri_json.decode("ASCII"))

        if not leveldb_has_key(self._db, blk_key) or not leveldb_has_key(self._db, compressed_key):
            raise Data_Not_Found_Error(
                Register._DISK_BLOCK_DATA_NOT_FOUND_ERROR_MSG_FULL.format(apri, start_n, length)
            )

        return blk_key, compressed_key

    def _check_blk_compressed_files_raise(self, blk_key, compressed_key, apri, start_n, length):

        blk_val = self._db.get(blk_key)
        compressed_val = self._db.get(compressed_key)

        blk_filename = self._local_dir / blk_val.decode("ASCII")

        if compressed_val != _IS_NOT_COMPRESSED_VAL:
            compressed_filename = self._local_dir / compressed_val.decode("ASCII")

            if not compressed_filename.exists() or not blk_filename.exists():
                raise Data_Not_Found_Error(
                    Register._DISK_BLOCK_DATA_NOT_FOUND_ERROR_MSG_FULL.format(str(apri), start_n, length)
                )

            return blk_filename, compressed_filename

        else:

            if not blk_filename.exists():
                raise Data_Not_Found_Error(
                    Register._DISK_BLOCK_DATA_NOT_FOUND_ERROR_MSG_FULL.format(str(apri), start_n, length)
                )

            return blk_filename, None

    @staticmethod
    def _check_apri_start_n_length_raise(apri, start_n, length):

        if not isinstance(apri, Apri_Info):
            raise TypeError("`apri` must be of type `Apri_Info`")

        if not is_int(start_n):
            raise TypeError("start_n` must be an `int`")
        else:
            start_n = int(start_n)

        if not is_int(length):
            raise TypeError("`length` must be an `int`")
        else:
            length = int(length)

        if not start_n >= 0:
            raise ValueError("`start_n` must be non-negative")

        if not length >= 0:
            raise ValueError("`length` must be non-negative")

        return start_n, length

    def _compress_helper_check_keys(self, compressed_key, apri, start_n, length):
        """Check status of the database and raise errors if anything is wrong.

        :param compressed_key: (type `bytes`) prefix is `_COMPRESSED_KEY_PREFIX`)
        :param apri: (type `Apri_Info`)
        :param start_n: (type `int`)
        :param length: (type `int`) non-negative
        :raise Compression_Error: If the `Block` has already been compressed.
        :raise Data_Not_Found_Error
        :return: (type `pathlib.Path`) The path of the data to compress.
        """

        blk_key, compressed_key = self._check_blk_compressed_keys_raise(
            None, compressed_key, apri, None, start_n, length
        )

        compressed_val = self._db.get(compressed_key)

        if compressed_val != _IS_NOT_COMPRESSED_VAL:
            raise Compression_Error(
                "The disk `Block` with the following data has already been compressed: " +
                f"{str(apri)}, start_n = {start_n}, length = {length}"
            )

        blk_filename = self._local_dir / self._db.get(blk_key).decode("ASCII")

        if not blk_filename.exists():
            raise Data_Not_Found_Error(
                Register._DISK_BLOCK_DATA_NOT_FOUND_ERROR_MSG_FULL.format(str(apri), start_n, length)
            )

        return blk_filename

    def _compress_helper_open_zipfile(self, compression_level):
        """Open a zip file with a random name. The handle must be closed manually.

        :return: (type `bytes`) If compression is successful, the appropriate compression key should be updated with
        this value.
        :return (type `pathlib.Path`) The path to the zip file.
        :return: (type `zipfile.ZipFile`) The zip file handle. This must be closed manually later.
        """

        compressed_filename = random_unique_filename(self._local_dir, COMPRESSED_FILE_SUFFIX)

        compressed_val = compressed_filename.name.encode("ASCII")

        compressed_fh = zipfile.ZipFile(
            compressed_filename, # target filename
            "x", # zip mode (write, but don't overwrite)
            zipfile.ZIP_DEFLATED, # compression mode
            True, # use zip64
            compression_level,
            strict_timestamps=False # change timestamps of old or new files
        )

        return compressed_val, compressed_filename, compressed_fh

    @staticmethod
    def _compress_helper_write_data(compressed_fh, blk_filename):
        """Compress the data.

        :param compressed_fh: (type `zipfile.ZipFile`)
        :param blk_filename: (type `pathlib.Path`)
        """

        compressed_fh.write(blk_filename, blk_filename.name)

    def _compress_helper_update_key(self, compressed_key, compressed_val):
        """If compression is successful, update the database.

        :param compressed_key: (type `bytes`)
        :param compressed_val: (type `bytes`)
        """

        self._db.put(compressed_key, compressed_val)

    @staticmethod
    def _compress_helper_clean_uncompressed_data(compressed_filename, blk_filename):
        """Remove uncompressed data after successful compression.

        :param compressed_filename: (type `pathlib.Path`)
        :param blk_filename: (type `pathlib.Path`) The uncompressed data to clean.
        """

        if compressed_filename.exists():

            if blk_filename.is_dir():
                shutil.rmtree(blk_filename)

            elif blk_filename.is_file():
                blk_filename.unlink(missing_ok = False)

            else:
                raise RuntimeError(f"Failed to delete uncompressed data at `{str(blk_filename)}`.")

            # make a ghost file with the same name so that `random_unique_filename` works as intended
            blk_filename.touch(exist_ok = False)

        else:
            raise Compression_Error(f"Failed to create zip file at `{str(compressed_filename)}`.")

    def _decompress_helper(self, apri, start_n, length):

        blk_key, compressed_key = self._check_blk_compressed_keys_raise(None, None, apri, None, start_n, length)

        compressed_val = self._db.get(compressed_key)

        if compressed_val == _IS_NOT_COMPRESSED_VAL:
            raise Decompression_Error(
                "The disk `Block` with the following data is not compressed: " +
                f"{str(apri)}, start_n = {start_n}, length = {length}"
            )

        blk_filename = self._db.get(blk_key).decode("ASCII")
        compressed_filename = self._local_dir / compressed_val.decode("ASCII")

        with zipfile.ZipFile(compressed_filename, "r") as compressed_fh:

            # delete ghost file
            (self._local_dir / blk_filename).unlink(False)

            try:
                blk_filename = compressed_fh.extract(blk_filename, self._local_dir)

            except RuntimeError as e:
                # ZipFile library doesn't define its errors very well so this is a catch-all exception for
                # extraction failure
                (self._local_dir / blk_filename).touch(exist_ok = False)
                raise e

            else:
                self._db.put(compressed_key, _IS_NOT_COMPRESSED_VAL)

        return compressed_filename, blk_filename

    #################################
    #    PUBLIC RAM BLK METHODS     #

    def add_ram_block(self, blk):

        if not isinstance(blk, Block):
            raise TypeError("`blk` must be of type `Block`.")

        if all(ram_blk is not blk for ram_blk in self._ram_blks):
            self._ram_blks.append(blk)

    def remove_ram_block(self, blk):

        if not isinstance(blk, Block):
            raise TypeError("`blk` must be of type `Block`.")

        for i, ram_blk in enumerate(self._ram_blks):
            if ram_blk is blk:
                del self._ram_blks[i]
                return

        raise Data_Not_Found_Error(f"No RAM disk block found.")

    def get_ram_block_by_n(self, apri, n, recursively = False):

        if not isinstance(apri, Apri_Info):
            raise TypeError("`apri` must be of type `Apri_Info`.")

        if not is_int(n):
            raise TypeError("`n` must be of type `int`.")
        else:
            n = int(n)

        if not isinstance(recursively, bool):
            raise TypeError("`recursively` must be of type `bool`.")

        if n < 0:
            raise IndexError("`n` must be non-negative")

        for blk in self._ram_blks:
            start_n = blk.get_start_n()
            if blk.get_apri() == apri and start_n <= n < start_n + len(blk):
                return blk

        if recursively:
            self._check_open_raise("get_ram_block_by_n")
            for subreg in self._iter_subregisters():
                with subreg._recursive_open() as subreg:
                    try:
                        return subreg.get_disk_block_by_n(apri, n, True)
                    except Data_Not_Found_Error:
                        pass

        raise Data_Not_Found_Error(
            Register._DISK_BLOCK_DATA_NOT_FOUND_ERROR_MSG_N.format(str(apri), n)
        )

    def get_all_ram_blocks(self, apri, recursively = False):

        if not isinstance(apri, Apri_Info):
            raise TypeError("`apri` must be of type `Apri_Info`.")

        if not isinstance(recursively, bool):
            raise TypeError("`recursively` must be of type `bool`.")

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
            not isinstance(short, tuple) or
            not(2 <= len(short) <= 3) or
            not(isinstance(short[0], Apri_Info)) or
            (not is_int(short[1]) and not isinstance(short[1], slice)) or
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
            for key, _ in self._iter_disk_block_pairs(_BLK_KEY_PREFIX, apri, None):
                apri, start_n, length = self._convert_disk_block_key(_BLK_KEY_PREFIX_LEN, key, apri)
                if start_n <= n < start_n + length:
                    break
            else:
                raise Data_Not_Found_Error(
                    Register._DISK_BLOCK_DATA_NOT_FOUND_ERROR_MSG_N.format(str(apri), n)
                )
            return self.get_disk_block(apri, start_n, length, recursively)[n]

    def get_all_intervals(self, apri, combine = True, recursively = False):

        if not isinstance(apri, Apri_Info):
            raise TypeError("`apri` must be of type `Apri_Info`.")

        if not isinstance(combine, bool):
            raise TypeError("`combine` must be of type `bool`.")

        if not isinstance(recursively, bool):
            raise TypeError("`recursively` must be of type `bool`.")

        intervals_sorted = sorted(
            [
                (start_n, length)
                for _, start_n, length in self._iter_converted_ram_and_disk_block_datas(apri, recursively)
            ],
            key = lambda t: (t[0], -t[1])
        )

        if combine:

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

            intervals_combined = []

            for start_n, length in intervals_reduced:

                if len(intervals_combined) == 0 or intervals_combined[-1][0] + intervals_combined[-1][1] < start_n:
                    intervals_combined.append((start_n, length))

                else:
                    intervals_combined[-1] = (intervals_combined[-1][0], start_n + length)

            return intervals_combined

        else:

            return intervals_sorted

    #################################
    # PROTEC RAM & DISK BLK METHODS #

    def _iter_converted_ram_and_disk_block_datas(self, apri, recursively = False):

        for blk in self._ram_blks:
            if blk.get_apri() == apri:
                yield apri, blk.get_start_n(), len(blk)

        for key, _ in self._iter_disk_block_pairs(_BLK_KEY_PREFIX, apri, None):
            yield self._convert_disk_block_key(_BLK_KEY_PREFIX_LEN, key, apri)

        if recursively:
            for subreg in self._iter_subregisters():
                with subreg._recursive_open() as subreg:
                    for data in subreg._iter_ram_and_disk_block_datas(apri, True):
                        yield data

class Pickle_Register(Register):

    @classmethod
    def dump_disk_data(cls, data, filename, **kwargs):

        if len(kwargs) > 0:
            raise KeyError("`Pickle_Register.add_disk_block` accepts no keyword-arguments.")

        filename = filename.with_suffix(".pkl")
        try:
            with filename.open("wb") as fh:
                pickle.dump(data, fh)

        except RuntimeError:
            raise OSError

        else:
            return filename

    @classmethod
    def load_disk_data(cls, filename, **kwargs):

        if len(kwargs) > 0:
            raise KeyError("`Pickle_Register.get_disk_block` accepts no keyword-arguments.")

        try:
            with filename.open("rb") as fh:
                return pickle.load(fh), filename

        except RuntimeError:
            raise OSError

Register.add_subclass(Pickle_Register)

class Numpy_Register(Register):

    @classmethod
    def dump_disk_data(cls, data, filename, **kwargs):

        if len(kwargs) > 0:
            raise KeyError("`Numpy_Register.add_disk_block` accepts no keyword-arguments.")


        filename = filename.with_suffix(".npy")
        try:
            np.save(filename, data, allow_pickle = False, fix_imports = False)

        except RuntimeError:
            raise OSError

        else:
            return filename

    @classmethod
    def load_disk_data(cls, filename, **kwargs):

        if "mmap_mode" in kwargs:
            mmap_mode = kwargs["mmap_mode"]

        else:
            mmap_mode = None

        if len(kwargs) > 1:
            raise KeyError("`Numpy_Register.get_disk_data` only accepts the keyword-argument `mmap_mode`.")

        if mmap_mode not in [None, "r+", "r", "w+", "c"]:
            raise ValueError(
                "The keyword-argument `mmap_mode` for `Numpy_Register.get_disk_block` can only have the values " +
                "`None`, 'r+', 'r', 'w+', 'c'. Please see " +
                "https://numpy.org/doc/stable/reference/generated/numpy.memmap.html#numpy.memmap for more information."
            )

        try:
            return np.load(filename, mmap_mode = mmap_mode, allow_pickle = False, fix_imports = False), filename

        except RuntimeError:
            raise OSError

    def get_disk_block(self, apri, start_n, length, return_metadata = False, recursively = False, **kwargs):
        """
        :param apri: (type `Apri_Info`)
        :param start_n: (type `int`)
        :param length: (type `length`) non-negative/
        :param return_metadata: (type `bool`, default `False`) Whether to return a `File_Metadata` object, which
        contains file creation date/time and size of dumped saved on the disk.
        :param recursively: (type `bool`, default `False`) Search all subregisters for the `Block`.
        :param mmap_mode: (type `str`, default `None`) Load the Numpy file using memory mapping, see
        https://numpy.org/doc/stable/reference/generated/numpy.memmap.html#numpy.memmap for more information.
        :return: (type `File_Metadata`) If `return_metadata is True`.
        """
        return super().get_disk_block(apri, start_n, length, return_metadata, recursively, **kwargs)

    def combine_blocks(self, apri, n_lower, n_upper, return_metadata = False):
        """Combine several `Block`s into a single `Block`, delete the old `Block`s, and save the new one to the disk.

        The interval `range(n_lower, n_upper)` must be the disjoint union of intervals of the form
        `range(blk.get_start_n(), blk.get_start_n() + len(blk))`, where `blk` is a disk `Block` with `Apri_Info` given by
        `apri`.

        :param apri: (type `Apri_Info`)
        :param n_lower: (type `int`)
        :param n_upper: (type `int`)
        :param return_metadata: (type `bool`, default `False`) Whether to return a `File_Metadata` object, which
        contains file creation date/time and size of dumped dumped to the disk.
        :raise Data_Not_Found_Data: If the union of the intervals of relevant disk `Block`s does not equal
        `range(n_lower, n_upper)`.
        :raise Value_Error: If any two intervals of relevant disk `Block`s intersect.
        :raise Value_Error: If any two relevant disk `Block` segments have inequal shapes.
        :return: (type `File_Metadata`) If `return_metadata is True`.
        """

        combined_interval = None

        last_check = False
        last_start_n = None

        intervals_to_get = []

        # this implementation depends on `get_disk_block_intervals` returning smaller start_n before larger
        # ones and, when ties occur, larger lengths before smaller ones.
        for start_n, length in self.get_disk_block_intervals(apri):

            if last_check:

                if last_start_n == start_n:
                    raise ValueError(f"Overlapping `Block` intervals found with {str(apri)}.")

                else:
                    break


            elif length > 0 and start_n >= n_lower:

                if start_n > n_lower and combined_interval is None:
                    raise Data_Not_Found_Error(
                        f"No disk `Block` found with the following data: {str(apri)}, start_n = {n_lower}"
                    )

                elif start_n == n_lower and combined_interval is None:
                    combined_interval = (start_n, start_n + length)
                    intervals_to_get.append((start_n, start_n + length))

                elif intervals_overlap((start_n, start_n + length), combined_interval):
                    raise ValueError(f"Overlapping `Block` intervals found with {str(apri)}.")

                elif start_n > combined_interval[1]:
                    raise Data_Not_Found_Error(
                        f"No `Block` found covering indices {combined_interval[1]} through {start_n} (inclusive) with "+
                        str(apri)
                    )

                elif start_n + length > n_upper:
                    raise Data_Not_Found_Error(
                        f"The last `Block` is too long. Try again by calling `reg.combine_blocks({str(apri)}, " +
                        f"{n_lower}, {start_n + length})`."
                    )

                else:
                    combined_interval = (n_lower, start_n + length)
                    intervals_to_get.append((start_n, start_n + length))

                if combined_interval == (n_lower, n_upper):
                    last_check = True
                    last_start_n = start_n

            elif length > 0 and intervals_overlap((start_n, start_n + length), (n_lower, n_upper)):
                raise ValueError(f"Overlapping `Block` intervals found with {str(apri)}.")

        else:

            if combined_interval != (n_lower, n_upper):
                raise Data_Not_Found_Error(
                    f"No `Block` found covering indices {combined_interval[1]} through {n_upper+1} (inclusive) with " +
                    str(apri)
                )

        blks = []

        fixed_shape = None
        ref_blk = None

        for start_n, length in intervals_to_get:

            blk = self.get_disk_block(apri, start_n, length, False, False, mmap_mode = "r")
            blks.append(blk)

            if fixed_shape is None:
                fixed_shape = blk.get_segment().shape[1:]
                ref_blk = blk

            elif fixed_shape != blk.get_segment().shape[1:]:
                raise ValueError(
                    "Cannot combine the following two `Block`s because all axes other than axis 0 must have the same " +
                    "shape:\n" +
                    f"{str(apri)}, start_n = {ref_blk.get_start_n()}, length = {len(ref_blk)}\n, shape = " +
                    f"{str(fixed_shape)}\n" +
                    f"{str(apri)}, start_n = {blk.get_start_n()}, length = {len(blk)}\n, shape = " +
                    f"{str(blk.get_segment().shape)}\n"

                )

        combined_blk = np.concatenate(blks, axis=0)
        combined_blk = Block(combined_blk, apri, n_lower)
        ret = self.add_disk_block(combined_blk, return_metadata)

        for blk in blks:
            self.remove_disk_block(apri, blk.get_start_n(), len(blk), False)

        return ret

Register.add_subclass(Numpy_Register)

class HDF5_Register(Register):

    @classmethod
    def dump_disk_data(cls, data, filename, **kwargs):
        pass

    @classmethod
    def load_disk_data(cls, filename, **kwargs):
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
        self.intervals = dict(self.reg.list_intervals_calculated(self.apri, self.recursively))

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