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
from contextlib import contextmanager
from pathlib import Path
from abc import ABC, abstractmethod

import lmdb
import numpy as np

from cornifer.errors import Data_Not_Found_Error, Register_Already_Open_Error, Register_Error, Compression_Error, \
    Decompression_Error, NOT_ABSOLUTE_ERROR_MESSAGE, Register_Recovery_Error
from cornifer.info import Apri_Info, Apos_Info
from cornifer.blocks import Block, Memmap_Block
from cornifer.file_metadata import File_Metadata
from cornifer._utilities import intervals_overlap, random_unique_filename, is_int, \
    resolve_path, BYTES_PER_MB, is_deletable
from cornifer._utilities.lmdb import lmdb_has_key, lmdb_prefix_iterator, open_lmdb, lmdb_is_closed, lmdb_count_keys
from cornifer.register_file_structure import VERSION_FILEPATH, LOCAL_DIR_CHARS, \
    COMPRESSED_FILE_SUFFIX, MSG_FILEPATH, CLS_FILEPATH, check_register_structure, DATABASE_FILEPATH, \
    REGISTER_FILENAME
from cornifer.version import CURRENT_VERSION, COMPATIBLE_VERSIONS

_NO_DEBUG = 0
_debug = _NO_DEBUG

#################################
#            LMDB KEYS          #

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
_APOS_KEY_PREFIX_LEN       = len(_APOS_KEY_PREFIX)

_IS_NOT_COMPRESSED_VAL     = b""

_SUB_VAL                   = b""

#################################
#        ERROR MESSAGES         #

_DISK_BLOCK_DATA_NOT_FOUND_ERROR_MSG_FULL = (
    "No disk block found with the following data: {0}, start_n = {1}, length = {2}."
)

_DISK_BLOCK_DATA_NOT_FOUND_ERROR_MSG_N = (
    "No disk block found with the following data: {0}, n = {1}."
)

_RAM_BLOCK_DATA_NOT_FOUND_ERROR_MSG_FULL = (
    "No RAM block found with the following data: {0}, start_n = {1}, length = {2}."
)

_RAM_BLOCK_DATA_NOT_FOUND_ERROR_MSG_N = (
    "No RAM block found with the following data: {0}, n = {1}."
)

_DISK_RAM_BLOCK_DATA_NOT_FOUND_ERROR_MSG_N = (
    "No disk nor RAM block found with the following data: {0}, n = {1}."
)

_NOT_CREATED_ERROR_MESSAGE = (
    "The `Register` database has not been created. You must do `with reg.open() as reg:` at least once before " +
    "calling the method `{0}`."
)

_MEMORY_FULL_ERROR_MESSAGE = (
    "Exceeded max `Register` size of {0}. Please increase the max size using the method `increase_register_size`."
)

#################################
#           CONSTANTS           #

_START_N_TAIL_LENGTH_DEFAULT   = 12
_START_N_HEAD_DEFAULT          =  0
_INITIAL_REGISTER_SIZE_DEFAULT = 5 * BYTES_PER_MB

class Register(ABC):

    #################################
    #     PUBLIC INITIALIZATION     #

    def __init__(self, saves_directory, message, initial_register_size = None):
        """This constructor neither creates nor opens a `Register` database. That is done by the context manager `open`.

        :param saves_directory: (type `str`)
        :param message: (type `str`) A brief message describing the data associated to this `Register`.
        :param initial_register_size: (type `int`) Size in bytes, the default is 5 MB. You may wish to set this lower
        than 5 MB if you do not expect to add many disk `Block`s to your register and you are concerned about disk
        memory. If your `Register` exceeds `initial_register_size`, then you can adjust the database size later via the
        method `increase_register_size`. If you are on a non-Windows system, there is no harm in setting this value
        to be very large (e.g. 1 TB).
        """

        if not isinstance(saves_directory, (str, Path)):
            raise TypeError("`saves_directory` must be a string or a `pathlib.Path`.")


        if not isinstance(message, str):
            raise TypeError("`message` must be a string.")

        if initial_register_size is not None and not is_int(initial_register_size):
            raise TypeError("`initial_register_size` must be of type `int`.")

        elif initial_register_size is not None:
            initial_register_size = int(initial_register_size)

        else:
            initial_register_size = _INITIAL_REGISTER_SIZE_DEFAULT

        if initial_register_size <= 0:
            raise ValueError("`initial_register_size` must be positive.")

        self.saves_directory = resolve_path(Path(saves_directory))

        if not self.saves_directory.is_dir():
            raise FileNotFoundError(
                f"You must create the file `{str(self.saves_directory)}` before calling "+
                f"`{self.__class__.__name__}(\"{str(self.saves_directory)}\", \"{message}\")`."
            )

        self._msg = message
        self._msg_filepath = None

        self._local_dir = None
        self._local_dir_bytes = None
        self._subreg_bytes = None

        self._db = None
        self._db_filepath = None
        self._db_map_size = initial_register_size
        self._read_only = None

        self._version = CURRENT_VERSION
        self._version_filepath = None

        self._cls_filepath = None

        self._start_n_head = _START_N_HEAD_DEFAULT
        self._start_n_tail_length = _START_N_TAIL_LENGTH_DEFAULT
        self._start_n_tail_mod = 10 ** self._start_n_tail_length

        self._ram_blks = []

        self._created = False

    @staticmethod
    def add_subclass(subclass):

        if not inspect.isclass(subclass):
            raise TypeError(f"The `subclass` argument must be a class, not a {type(subclass)}.")

        if not issubclass(subclass, Register):
            raise TypeError(f"The class `{subclass.__name__}` must be a subclass of `Register`.")

        Register._constructors[subclass.__name__] = subclass

    #################################
    #     PROTEC INITIALIZATION     #

    _constructors = {}

    _instances = {}

    @staticmethod
    def _from_local_dir(local_dir):
        """Return a `Register` instance from a `local_dir` with the correct concrete subclass.

        This static method does not open the `Register` database at any point.

        :param local_dir: (type `pathlib.Path`) Absolute.
        :return: (type `Register`)
        """

        if not local_dir.is_absolute():
            raise ValueError(NOT_ABSOLUTE_ERROR_MESSAGE.format(str(local_dir)))

        if not local_dir.exists():
            raise FileNotFoundError(f"The `Register` database `{str(local_dir)}` could not be found.")

        check_register_structure(local_dir)

        if Register._instance_exists(local_dir):

            # return the `Register` that has already been opened
            return Register._get_instance(local_dir)

        else:

            with (local_dir / CLS_FILEPATH).open("r") as fh:
                cls_name = fh.read()

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

            with (local_dir / MSG_FILEPATH).open("r") as fh:
                msg = fh.read()

            reg = con(local_dir.parent, msg)

            reg._set_local_dir(local_dir)

            with (local_dir / VERSION_FILEPATH).open("r") as fh:
                reg._version = fh.read()

            return reg

    @staticmethod
    def _add_instance(local_dir, reg):
        """
        :param local_dir: (type `pathlib.Path`) Absolute.
        :param reg: (type `Register`)
        """

        if not local_dir.is_absolute():
            raise ValueError(NOT_ABSOLUTE_ERROR_MESSAGE.format(str(local_dir)))

        Register._instances[local_dir] = reg

    @staticmethod
    def _instance_exists(local_dir):
        """
        :param local_dir: (type `pathlib.Path`) Absolute.
        :return: (type `bool`)
        """

        if not local_dir.is_absolute():
            raise ValueError(NOT_ABSOLUTE_ERROR_MESSAGE.format(str(local_dir)))

        return local_dir in Register._instances.keys()

    @staticmethod
    def _get_instance(local_dir):
        """
        :param local_dir: (type `pathlib.Path`) Absolute.
        :return: (type `Register`)
        """

        if not local_dir.is_absolute():
            raise ValueError(NOT_ABSOLUTE_ERROR_MESSAGE.format(str(local_dir)))

        return Register._instances[local_dir]

    #################################
    #    PUBLIC REGISTER METHODS    #

    def __eq__(self, other):

        if not self._created or not other._created:
            raise Register_Error(_NOT_CREATED_ERROR_MESSAGE.format("__eq__"))

        elif type(self) != type(other):
            return False

        else:
            return self._local_dir == other._local_dir

    def __hash__(self):

        if not self._created:
            raise Register_Error(_NOT_CREATED_ERROR_MESSAGE.format("__hash__"))

        else:
            return hash(str(self._local_dir)) + hash(type(self))

    def __str__(self):
        return self._msg

    def __repr__(self):
        return f"{self.__class__.__name__}(\"{str(self.saves_directory)}\", \"{self._msg}\")"

    def __contains__(self, apri):

        self._check_open_raise("__contains__")

        if any(blk.apri() == apri for blk in self._ram_blks):
            return True

        else:
            return lmdb_has_key(self._db, _APRI_ID_KEY_PREFIX + apri.to_json().encode("ASCII"))

    def __iter__(self):

        with lmdb_prefix_iterator(self._db, _ID_APRI_KEY_PREFIX) as it:

            for _, apri_json in it:
                yield Apri_Info.from_json(apri_json.decode("ASCII"))

    def set_message(self, message):
        """Give this `Register` a brief description.

        WARNING: This method OVERWRITES the current message. In order to append a new message to the current one, do
        something like the following:

            old_message = str(reg)
            new_message = old_message + " Hello!"
            reg.set_message(new_message)

        :param message: (type `str`)
        """

        if not isinstance(message, str):
            raise TypeError("`message` must be a string.")

        self._msg = message

        if self._created:
            with self._msg_filepath.open("w") as fh:
                fh.write(message)

    def set_start_n_info(self, head = None, tail_length = None):
        """Set the range of the `start_n` parameters of disk `Block`s belonging to this `Register`.

        Reset to default `head` and `tail_length` by omitting the parameters.

        If the `start_n` parameter is very large (of order more than trillions), then the `Register` database can
        become very bloated by storing many redundant digits for the `start_n` parameter. Calling this method with
        appropriate `head` and `tail_length` parameters alleviates the bloat.

        The "head" and "tail" of a non-negative number x is defined by x = head * 10^L + tail, where L is the "length",
        or the number of digits, of "tail". (L must be at least 1, and 0 is considered to have 1 digit.)

        By calling `set_start_n_info(head, tail_length)`, the user is asserting that the start_n of every disk
        `Block` belong to this `Register` can be decomposed in the fashion start_n = head * 10^tail_length + tail. The
        user is discouraged to call this method for large `tail_length` values (>12), as this is likely unnecessary and
        defeats the purpose of this method.

        :param head: (type `int`, optional) Non-negative. If omitted, resets this `Register` to the default `head`.
        :param tail_length: (type `int`) Positive. If omitted, resets this `Register` to the default `tail_length`.
        """

        # DEBUG : 1, 2

        self._check_open_raise("set_start_n_info")

        self._check_readwrite_raise("set_start_n_info")

        if head is not None and not is_int(head):
            raise TypeError("`head` must be of type `int`.")

        elif head is not None:
            head = int(head)

        else:
            head = _START_N_HEAD_DEFAULT

        if tail_length is not None and not is_int(tail_length):
            raise TypeError("`tail_length` must of of type `int`.")

        elif tail_length is not None:
            tail_length = int(tail_length)

        else:
            tail_length = _START_N_TAIL_LENGTH_DEFAULT

        if head < 0:
            raise ValueError("`head` must be non-negative.")

        if tail_length <= 0:
            raise ValueError("`tail_length` must be positive.")

        if head == self._start_n_head and tail_length == self._start_n_tail_length:
            return

        new_mod = 10 ** tail_length

        with lmdb_prefix_iterator(self._db, _BLK_KEY_PREFIX) as it:

            for key, _ in it:

                apri, start_n, length = self._convert_disk_block_key(_BLK_KEY_PREFIX_LEN, key)

                if start_n // new_mod != head:

                    raise ValueError(
                        "The following `start_n` does not have the correct head:\n" +
                        f"`start_n`   : {start_n}\n" +
                        "That `start_n` is associated with a `Block` whose `Apri_Info` and length is:\n" +
                        f"`Apri_Info` : {str(apri.to_json())}\n" +
                        f"length      : {length}\n"
                    )

        if _debug == 1:
            raise KeyboardInterrupt

        try:

            with self._db.begin(write = True) as rw_txn:

                with self._db.begin() as ro_txn:

                    with lmdb_prefix_iterator(ro_txn, _BLK_KEY_PREFIX) as it:

                        rw_txn.put(_START_N_HEAD_KEY, str(head).encode("ASCII"))
                        rw_txn.put(_START_N_TAIL_LENGTH_KEY, str(tail_length).encode("ASCII"))

                        for key, val in it:

                            _, start_n, _ = self._convert_disk_block_key(_BLK_KEY_PREFIX_LEN, key)
                            apri_json, _, length_bytes = Register._split_disk_block_key(_BLK_KEY_PREFIX_LEN, key)

                            new_start_n_bytes = str(start_n % new_mod).encode("ASCII")

                            new_key = Register._join_disk_block_data(
                                _BLK_KEY_PREFIX, apri_json, new_start_n_bytes, length_bytes
                            )

                            if key != new_key:

                                rw_txn.put(new_key, val)
                                rw_txn.delete(key)

                if _debug == 2:
                    raise KeyboardInterrupt

        except lmdb.MapFullError as e:
            raise Register_Error(_MEMORY_FULL_ERROR_MESSAGE.format(self._db_map_size)) from e

        self._start_n_head = head
        self._start_n_tail_length = tail_length
        self._start_n_tail_mod = 10 ** self._start_n_tail_length

    @contextmanager
    def open(self, read_only = False):

        if not self._created and not read_only:

            # set local directory info and create levelDB database
            local_dir = random_unique_filename(self.saves_directory, length = 4, alphabet = LOCAL_DIR_CHARS)

            try:

                local_dir.mkdir(exist_ok = False)
                (local_dir / REGISTER_FILENAME).mkdir(exist_ok = False)

                with (local_dir / MSG_FILEPATH).open("x") as fh:
                    fh.write(self._msg)

                with (local_dir / VERSION_FILEPATH).open("x") as fh:
                    fh.write(self._version)

                with (local_dir / CLS_FILEPATH).open("x") as fh:
                    fh.write(str(type(self).__name__))

                (local_dir / DATABASE_FILEPATH).mkdir(exist_ok = False)

                self._set_local_dir(local_dir)

                self._db = open_lmdb(self._db_filepath, self._db_map_size, False)

                try:

                    with self._db.begin(write = True) as txn:
                        # set register info
                        txn.put(_START_N_HEAD_KEY, str(self._start_n_head).encode("ASCII"))
                        txn.put(_START_N_TAIL_LENGTH_KEY, str(self._start_n_tail_length).encode("ASCII"))
                        txn.put(_CURR_ID_KEY, b"0")

                except lmdb.MapFullError as e:
                    raise Register_Error(_MEMORY_FULL_ERROR_MESSAGE.format(self._db_map_size)) from e

                Register._add_instance(local_dir, self)
                yiel = self

            except BaseException as e:

                if local_dir.is_dir():
                    shutil.rmtree(local_dir)

                raise e

        elif self._created:
            yiel = self._open_created(read_only)

        else:
            raise ValueError(
                "You must `open` this `Register` at least once with `read_only = False` before you can open it in "
                "read-only mode."
            )

        try:
            yield yiel

        finally:
            yiel._close_created()

    def increase_register_size(self, num_bytes):
        """WARNING: DO NOT CALL THIS METHOD FROM MORE THAN ONE PYTHON PROCESS AT A TIME. You are safe if you call it
        from only one Python process. You are safe if you have multiple Python processes running and call it from only
        ONE of them. But do NOT call it from multiple processes at once. Doing so may result in catastrophic loss of
        data.

        :param num_bytes: (type `int`) Positive.
        """

        self._check_open_raise("increase_register_size")

        if not is_int(num_bytes):
            raise TypeError("`num_bytes` must be of type `int`.")

        if num_bytes <= 0:
            raise ValueError("`num_bytes` must be positive.")

        if num_bytes <= self._db_map_size:
            raise ValueError("`num_bytes` must be larger than the current `Register` size.")

        self._db.set_mapsize(num_bytes)
        self._db_map_size = num_bytes

    def register_size(self):
        return self._db_map_size

    #################################
    #    PROTEC REGISTER METHODS    #

    def _open_created(self, read_only):

        if Register._instance_exists(self._local_dir):
            ret = Register._get_instance(self._local_dir)
        else:
            ret = self

        if not ret._created:
            raise Register_Error(_NOT_CREATED_ERROR_MESSAGE.format("_open_created"))

        if ret._db is not None and not ret._db_is_closed():
            raise Register_Already_Open_Error()

        self._read_only = read_only

        ret._db = open_lmdb(self._db_filepath, self._db_map_size, read_only)

        return ret

    def _close_created(self):
        self._db.close()

    @contextmanager
    def _recursive_open(self, read_only):

        if not self._created:
            raise Register_Error(_NOT_CREATED_ERROR_MESSAGE.format("_recursive_open"))

        else:

            try:
                yiel = self._open_created(read_only)
                need_close = True

            except Register_Already_Open_Error:
                yiel = self
                need_close = False

            if not read_only and yiel._read_only:
                raise ValueError(
                    "Attempted to open a `Register` in read-write mode that is already open in read-only mode."
                )

            try:
                yield yiel

            finally:
                if need_close:
                    yiel._close_created()

    def _check_open_raise(self, method_name):

        if self._db is None or self._db_is_closed():
            raise Register_Error(
                f"This `Register` database has not been opened. You must open this register via `with reg.open() as " +
                f"reg:` before calling the method `{method_name}`."
            )

    def _check_readwrite_raise(self, method_name):
        """Call `self._check_open_raise` before this method."""

        if self._read_only:
            raise Register_Error(
                f"This `Register` is `open`ed in read-only mode. In order to call the method `{method_name}`, you must "
                "open this `Register` in read-write mode via `with reg.open() as reg:`."
            )

    # def _check_memory_raise(self, keys, vals):
    #
    #     stat = self._db.stat()
    #
    #     current_size = stat.psize * (stat.leaf_pages + stat.branch_pages + stat.overflow_pages)
    #
    #     entry_size_bytes = sum(len(key) + len(val) for key, val in zip(keys, vals)) * BYTES_PER_CHAR
    #
    #     if current_size + entry_size_bytes >= Register._MEMORY_FULL_PROP * self._db_map_size:
    #
    #         raise MemoryError(
    #             "The `Register` database is out of memory. Please allocate more memory using the method "
    #             "`Register.increase_register_size`."
    #         )

    def _set_local_dir(self, local_dir):
        """`local_dir` and a corresponding register database must exist prior to calling this method.

        :param local_dir: (type `pathlib.Path`) Absolute.
        """

        if not local_dir.is_absolute():
            raise ValueError(NOT_ABSOLUTE_ERROR_MESSAGE.format(str(local_dir)))

        if local_dir.parent != self.saves_directory:
            raise ValueError(
                "The `local_dir` argument must be a sub-directory of `reg.saves_directory`.\n" +
                f"`local_dir.parent`    : {str(local_dir.parent)}\n"
                f"`reg.saves_directory` : {str(self.saves_directory)}"
            )

        check_register_structure(local_dir)

        self._created = True

        self._local_dir = local_dir
        self._local_dir_bytes = str(self._local_dir).encode("ASCII")

        self._db_filepath = self._local_dir / DATABASE_FILEPATH

        self._subreg_bytes = (
            _SUB_KEY_PREFIX + self._local_dir_bytes
        )

        self._version_filepath = local_dir / VERSION_FILEPATH
        self._msg_filepath = local_dir / MSG_FILEPATH
        self._cls_filepath = local_dir / CLS_FILEPATH

    def _has_compatible_version(self):
        return self._version in COMPATIBLE_VERSIONS

    def _db_is_closed(self):

        if not self._created:
            raise Register_Error(_NOT_CREATED_ERROR_MESSAGE.format("_db_is_closed"))

        else:
            return lmdb_is_closed(self._db)

    #################################
    #      PUBLIC APRI METHODS      #

    def apri_infos(self, recursively = False):

        if not isinstance(recursively, bool):
            raise TypeError("`recursively` must be of type `bool`")

        ret = []
        for blk in self._ram_blks:
            ret.append(blk.apri())

        self._check_open_raise("apri_infos")

        with lmdb_prefix_iterator(self._db, _ID_APRI_KEY_PREFIX) as it:
            for _, val in it:
                ret.append(Apri_Info.from_json(val.decode("ASCII")))


        if recursively:
            for subreg in self._iter_subregisters():
                with subreg._recursive_open(True) as subreg:
                    ret.append(subreg.apri_infos())

        return sorted(list(set(ret)))

    def change_apri_info(self, old_apri, new_apri, recursively = False):
        """Replace an old `Apri_Info`, and all references to it, with a new `Apri_Info`.

        If ANY `Block`, `Apri_Info`, or `Apos_Info` references `old_apri`, its entries in this `Register` will be
        updated to reflect the replacement of `old_apri` with `new_apri`. (See example below.) After the replacement
        `old_apri` -> `new_apri` is made, the set of `Apri_Info` that changed under that replacement must be disjoint
        from the set of `Apri_Info` that did not change. Otherwise, a `ValueError` is raised.

        For example, say we intend to replace

        `old_apri = Apri_Info(descr = "periodic points")`

        with

        `new_apri = Apri_info(descr = "odd periods", ref = "Newton et al. 2005")`.

        In an example `Register`, there are two `Block`s, one with `old_apri` and the other with

        `some_other_apri = Apri_info(descr = "period length", respective = old_apri)`.

        After a call to `change_apri_info(old_apri, new_apri)`, the first `Block` will have `new_apri` and the second
        will have

        `Apri_Info(descr = "period length", respective = new_apri)`.

        :param old_apri: (type `Apri_Info`)
        :param new_apri: (type `Apri_info`)
        :param recursively: (type `bool`)
        :raise ValueError: See above.
        """

        # DEBUG : 1, 2, 3

        self._check_open_raise("change_apri_info")

        self._check_readwrite_raise("change_apri_info")

        # raises `Data_Not_Found_Error` if `old_apri` does not have an ID
        old_apri_id = self._get_id_by_apri(old_apri, None, False)

        if old_apri == new_apri:
            return

        old_apri_json = old_apri.to_json().encode("ASCII")

        old_apri_id_key = _APRI_ID_KEY_PREFIX + old_apri_json
        old_id_apri_key = _ID_APRI_KEY_PREFIX + old_apri_id

        new_apri_json = new_apri.to_json().encode("ASCII")

        if lmdb_has_key(self._db, _APRI_ID_KEY_PREFIX + new_apri_json):

            new_apri_id = self._get_id_by_apri(new_apri, new_apri_json, False)
            new_id_apri_key = _ID_APRI_KEY_PREFIX + new_apri_id
            has_new_apri_already = True

            warnings.warn(f"This `Register` already has a reference to {str(new_apri)}.")

        else:

            new_apri_id = None
            new_id_apri_key = None
            has_new_apri_already = False

        if _debug == 1:
            raise KeyboardInterrupt

        try:

            with self._db.begin(write = True) as rw_txn:

                with self._db.begin() as ro_txn:

                    apris_changed = set()
                    apris_didnt_change = set()

                    # change all apri_id keys
                    with lmdb_prefix_iterator(ro_txn, _APRI_ID_KEY_PREFIX) as it:
                        for key, val in it:

                            if key == old_apri_id_key:
                                new_key = _APRI_ID_KEY_PREFIX + new_apri_json

                            else:

                                apri = Apri_Info.from_json(key[_APRI_ID_KEY_PREFIX_LEN : ].decode("ASCII"))
                                replaced = apri.change_info(old_apri, new_apri)
                                new_key = _APRI_ID_KEY_PREFIX + replaced.to_json().encode("ASCII")

                            if new_key != key:

                                rw_txn.put(new_key, val)
                                rw_txn.delete(key)
                                apris_changed.add(new_key[_APRI_ID_KEY_PREFIX_LEN : ])

                            else:
                                apris_didnt_change.add(key[_APRI_ID_KEY_PREFIX_LEN : ])

                    # check `apris_changed` and `apris_didnt_change` are disjoint, otherwise raise ValueError
                    if not apris_changed.isdisjoint(apris_didnt_change):

                        # ValueError automatically aborts the LMDB transaction
                        raise ValueError(
                            "The set of `Apri_Info` that changed under the replacement `old_apri` -> `new_apri` must be "
                            "disjoint from the set of `Apri_Info` that did not change."
                        )

                    # change all id_apri keys
                    with lmdb_prefix_iterator(ro_txn, _ID_APRI_KEY_PREFIX) as it:
                        for key, val in it:

                            new_key = key

                            if key == old_id_apri_key:
                                new_val = new_apri_json

                            else:

                                apri = Apri_Info.from_json(val.decode("ASCII"))
                                replaced = apri.change_info(old_apri, new_apri)
                                new_val = replaced.to_json().encode("ASCII")

                            if has_new_apri_already and key == new_id_apri_key:
                                new_key = old_id_apri_key

                            if key != new_key or val != new_val:
                                rw_txn.put(new_key, new_val)

                    if has_new_apri_already:

                        # change all blocks
                        for prefix in [_BLK_KEY_PREFIX, _COMPRESSED_KEY_PREFIX]:

                            with lmdb_prefix_iterator(ro_txn, prefix + new_apri_id + _KEY_SEP) as it:
                                for key, val in it:

                                    new_blk_key = prefix + old_apri_id + key[key.index(_KEY_SEP) : ]
                                    rw_txn.put(new_blk_key, val)

                    # change all apos vals
                    with lmdb_prefix_iterator(ro_txn, _APOS_KEY_PREFIX) as it:
                        for key, val in it:

                            apos = Apos_Info.from_json(val.decode("ASCII"))
                            replaced = apos.change_info(old_apri, new_apri)
                            new_val = replaced.to_json().encode("ASCII")

                            if val != new_val:
                                rw_txn.put(new_key, new_val)

                    if _debug == 2:
                        raise KeyboardInterrupt

                if _debug == 3:
                    raise KeyboardInterrupt

        except lmdb.MapFullError as e:
            raise Register_Error(_MEMORY_FULL_ERROR_MESSAGE.format(self._db_map_size)) from e

        if recursively:
            for subreg in self._iter_subregisters():
                with subreg._recursive_open(False) as subreg:
                    subreg.change_apri_info(old_apri, new_apri, True)

    def remove_apri_info(self, apri):
        """Remove an `Apri_Info` that is not associated with any other `Apri_Info`, `Block`, nor `Apos_Info`.

        :param apri: (type `Apri_Info`)
        :raise ValueError: If there are any `Apri_Info`, `Block`, or `Apos_Info` associated with `apri`.
        """

        # DEBUG : 1, 2, 3, 4

        self._check_open_raise("remove_apri_info")

        self._check_readwrite_raise("remove_apri_info")

        if not isinstance(apri, Apri_Info):
            raise TypeError("`apri` must be of type `Apri_Info`.")

        _id = self._get_id_by_apri(apri, None, False)

        if self.num_disk_blocks(apri) != 0:
            raise ValueError(
                f"There are disk `Block`s saved with `{str(apri)}`. Please remove them first and call "
                "`remove_apri_info` again."
            )

        if _debug == 1:
            raise KeyboardInterrupt

        with lmdb_prefix_iterator(self._db, _ID_APRI_KEY_PREFIX) as it:

            for _, _apri_json in it:

                _apri = Apri_Info.from_json(_apri_json.decode("ASCII"))

                if apri in _apri and apri != _apri:

                    raise ValueError(
                        f"{str(_apri)} is associated with {str(apri)}. Please remove the former first before removing "
                        "the latter."
                    )

        if _debug == 2:
            raise KeyboardInterrupt

        try:
            self.apos_info(apri)

        except Data_Not_Found_Error:
            pass

        else:
            raise ValueError(
                f"There is an `Apos_Info` associated with `{str(apri)}`. Please remove it first and call "
                "`remove_apri_info` again."
            )

        if _debug == 3:
            raise KeyboardInterrupt

        try:

            with self._db.begin(write = True) as txn:

                txn.delete(_ID_APRI_KEY_PREFIX + _id)
                txn.delete(_APRI_ID_KEY_PREFIX + apri.to_json().encode("ASCII"))

                if _debug == 4:
                    raise KeyboardInterrupt

        except lmdb.MapFullError as e:
            raise Register_Error(_MEMORY_FULL_ERROR_MESSAGE.format(self._db_map_size)) from e

    #################################
    #      PROTEC APRI METHODS      #

    def _get_apri_json_by_id(self, _id, txn = None):
        """Get JSON bytestring representing an `Apri_Info` instance.

        :param _id: (type `bytes`)
        :param txn: (type `lmbd.Transaction`, default `None`) The transaction to query. If `None`, then use open a new
        transaction and commit it after this method resolves.
        :return: (type `bytes`)
        """

        commit = txn is None

        if commit:
            txn = self._db.begin()

        try:
            return txn.get(_ID_APRI_KEY_PREFIX + _id)

        finally:
            if commit:
                txn.commit()

    def _get_id_by_apri(self, apri, apri_json, missing_ok, txn = None):
        """Get an `Apri_Info` ID for this database. If `missing_ok is True`, then create an ID if the passed `apri` or
        `apri_json` is unknown to this `Register`.

        One of `apri` and `apri_json` can be `None`, but not both. If both are not `None`, then `apri` is used.

        `self._db` must be opened by the caller.

        :param apri: (type `Apri_Info`)
        :param apri_json: (type `bytes`)
        :param missing_ok: (type `bool`) Create an ID if the passed `apri` or `apri_json` is unknown to this `Register`.
        :param txn: (type `lmbd.Transaction`, default `None`) The transaction to query. If `None`, then use open a new
        transaction and commit it after this method resolves.
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

        commit = txn is None

        if commit and missing_ok:
            txn = self._db.begin(write = True)

        elif commit:
            txn = self._db.begin()

        try:
            _id = txn.get(key, default = None)

            if _id is not None:
                return _id

            elif missing_ok:

                _id = txn.get(_CURR_ID_KEY)
                next_id = str(int(_id) + 1).encode("ASCII")

                txn.put(_CURR_ID_KEY, next_id)
                txn.put(key, _id)
                txn.put(_ID_APRI_KEY_PREFIX + _id, key[_APRI_ID_KEY_PREFIX_LEN : ])

                return _id

            else:

                if apri is None:
                    apri = Apri_Info.from_json(apri_json.decode("ASCII"))

                raise Data_Not_Found_Error(f"`{str(apri)}` is not known to this `Register`.")

        finally:

            if commit:

                try:
                    txn.commit()

                except lmdb.MapFullError as e:
                    raise Register_Error(_MEMORY_FULL_ERROR_MESSAGE.format(self._db_map_size)) from e

    #################################
    #      PUBLIC APOS METHODS      #

    def set_apos_info(self, apri, apos):
        """Set some `Apos_Info` for corresponding `Apri_Info`.

        WARNING: This method will OVERWRITE any previous saved `Apos_Info`. If you do not want to lose any previously
        saved data, then you should do something like the following:

            apos = reg.apos_info(apri)
            apos.period_length = 5
            reg.set_apos_info(apos)

        :param apri: (type `Apri_Info`)
        :param apos: (type `Apos_Info`)
        """

        # DEBUG : 1, 2

        self._check_open_raise("set_apos_info")

        self._check_readwrite_raise("set_apos_info")

        if not isinstance(apri, Apri_Info):
            raise TypeError("`apri` must be of type `Apri_Info`")

        if not isinstance(apos, Apos_Info):
            raise TypeError("`apos` must be of type `Apos_Info`")

        key = self._get_apos_key(apri, None, True)
        apos_json = apos.to_json().encode("ASCII")

        if _debug == 1:
            raise KeyboardInterrupt

        try:

            with self._db.begin(write = True) as txn:

                txn.put(key, apos_json)

                if _debug == 2:
                    raise KeyboardInterrupt

        except lmdb.MapFullError as e:
            raise Register_Error(_MEMORY_FULL_ERROR_MESSAGE.format(self._db_map_size)) from e

    def apos_info(self, apri):
        """Get some `Apos_Info` associated with a given `Apri_Info`.

        :param apri: (type `Apri_Info`)
        :raises Data_Not_Found_Error: If no `Apos_Info` has been associated to `apri`.
        :return: (type `Apos_Info`)
        """

        self._check_open_raise("apos_info")

        if not isinstance(apri, Apri_Info):
            raise TypeError("`apri` must be of type `Apri_Info`")

        key = self._get_apos_key(apri, None, False)

        with self._db.begin() as txn:
            apos_json = txn.get(key, default=None)

        if apos_json is not None:
            return Apos_Info.from_json(apos_json.decode("ASCII"))

        else:
            raise Data_Not_Found_Error(f"No `Apos_Info` associated with `{str(apri)}`.")

    def remove_apos_info(self, apri):

        # DEBUG : 1, 2

        self._check_open_raise("remove_apos_info")

        self._check_readwrite_raise("remove_apos_info")

        if not isinstance(apri, Apri_Info):
            raise TypeError("`apri` must be of type `Apri_Info`.")

        key = self._get_apos_key(apri, None, False)

        if _debug == 1:
            raise KeyboardInterrupt

        if lmdb_has_key(self._db, key):

            try:

                with self._db.begin(write = True) as txn:

                    txn.delete(key)

                    if _debug == 2:
                        raise KeyboardInterrupt

            except lmdb.MapFullError as e:
                raise Register_Error(_MEMORY_FULL_ERROR_MESSAGE.format(self._db_map_size)) from e

        else:
            raise Data_Not_Found_Error(f"No `Apos_Info` associated with `{str(apri)}`.")

    #################################
    #      PROTEC APOS METHODS      #

    def _get_apos_key(self, apri, apri_json, missing_ok, txn = None):
        """Get a key for an `Apos_Info` entry.

        One of `apri` and `apri_json` can be `None`, but not both. If both are not `None`, then `apri` is used. If
        `missing_ok is True`, then create a new `Apri_Info` ID if one does not already exist for `apri`.

        :param apri: (type `Apri_Info`)
        :param apri_json: (type `bytes`)
        :param missing_ok: (type `bool`)
        :param txn: (type `lmbd.Transaction`, default `None`) The transaction to query. If `None`, then use open a new
        transaction and commit it after this method resolves.
        :raises Apri_Info_Not_Found_Error: If `missing_ok is False` and `apri` is not known to this `Register`.
        :return: (type `bytes`)
        """

        if apri is None and apri_json is None:
            raise ValueError

        apri_id = self._get_id_by_apri(apri, apri_json, missing_ok, txn)

        return _APOS_KEY_PREFIX + _KEY_SEP + apri_id

    #################################
    #  PUBLIC SUB-REGISTER METHODS  #

    def add_subregister(self, subreg):

        # DEBUG : 1, 2

        self._check_open_raise("add_subregister")

        self._check_readwrite_raise("add_subregister")

        if not isinstance(subreg, Register):
            raise TypeError("`subreg` must be of a `Register` derived type")

        if not subreg._created:
            raise Register_Error(_NOT_CREATED_ERROR_MESSAGE.format("add_subregister"))

        key = subreg._get_subregister_key()

        if _debug == 1:
            raise KeyboardInterrupt

        if not lmdb_has_key(self._db, key):

            if subreg._check_no_cycles_from(self):

                try:

                    with self._db.begin(write = True) as txn:

                        txn.put(key, _SUB_VAL)

                        if _debug == 2:
                            raise KeyboardInterrupt

                except lmdb.MapFullError as e:
                    raise Register_Error(_MEMORY_FULL_ERROR_MESSAGE.format(self._db_map_size)) from e

            else:

                raise Register_Error(
                    "Attempting to add this register as a sub-register will created a directed cycle in the " +
                    "subregister relation. "
                    f'Intended super-register description: "{str(self)}". '
                    f'Intended sub-register description: "{str(subreg)}".'
                )

        else:
            raise Register_Error("`Register` already added as subregister.")

    def remove_subregister(self, subreg):
        """
        :param subreg: (type `Register`)
        """

        # DEBUG : 1, 2

        self._check_open_raise("remove_subregister")

        self._check_readwrite_raise("remove_subregister")

        if not isinstance(subreg, Register):
            raise TypeError("`subreg` must be of a `Register` derived type.")

        key = subreg._get_subregister_key()

        if _debug == 1:
            raise KeyboardInterrupt

        if lmdb_has_key(self._db, key):

            try:

                with self._db.begin(write = True) as txn:

                    txn.delete(key)

                    if _debug == 2:
                        raise KeyboardInterrupt

            except lmdb.MapFullError as e:
                raise Register_Error(_MEMORY_FULL_ERROR_MESSAGE.format(self._db_map_size)) from e


        else:
            raise Register_Error("`Register` not added as subregister.")

    #################################
    #  PROTEC SUB-REGISTER METHODS  #

    def _check_no_cycles_from(self, original, touched = None):
        """Checks if adding `self` as a subregister to `original` would not create any directed cycles containing the
        arc `original` -> `self` in the subregister relation.

        Returns `False` if a directed cycle would be created and `True` otherwise. If `self` is already a subregister
        of `original`, then return `True` if the currently existing relation has no directed cycles that pass through
        `self`, and `False` otherwise. If `self == original`, then return `False`.

        :param original: (type `Register`)
        :param touched: used for recursion.
        :return: (type `bool`)
        """

        if not self._created or not original._created:
            raise Register_Error(_NOT_CREATED_ERROR_MESSAGE.format("_check_no_cycles_from"))

        if self is original:
            return False

        if touched is None:
            touched = set()

        with self._recursive_open(True) as reg:

            if any(
                original is subreg
                for subreg in reg._iter_subregisters()
            ):
                return False

            for subreg in reg._iter_subregisters():

                if subreg not in touched:

                    touched.add(subreg)
                    if not subreg._check_no_cycles_from(original, touched):
                        return False


            else:
                return True

    def _iter_subregisters(self):

        with lmdb_prefix_iterator(self._db, _SUB_KEY_PREFIX) as it:

            for key, _ in it:

                local_dir = Path(key[_SUB_KEY_PREFIX_LEN : ].decode("ASCII"))
                subreg = Register._from_local_dir(local_dir)
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
        not an instance-method. It merely takes `data`, processes it, and dumps it to disk.

        Most use-cases prefer the instance-method `add_disk_block`.

        :param data: (any type) The raw data to dump.
        :param filename: (type `pathlib.Path`) The filename to dump to. You may edit this filename if
        necessary (such as by adding a suffix), but you must return the edited filename.
        :return: (type `pathlib.Path`) The actual filename of the data on the disk.
        """

    @classmethod
    @abstractmethod
    def load_disk_data(cls, filename, **kwargs):
        """Load raw data from the disk.

        This method should not change any properties of any `Register`, which is why it is a classmethod and
        not an instancemethod. It merely loads the data saved on the disk, processes it, and returns it.

        Most use-cases prefer the method `disk_block`.

        :param filename: (type `pathlib.Path`) Where to load the block from. You may need to edit this
        filename if necessary, such as by adding a suffix, but you must return the edited filename.
        :raises Data_Not_Found_Error: If the data could not be loaded because it doesn't exist.
        :return: (any type) The data loaded from the disk.
        :return: (pathlib.Path) The exact path of the data saved to the disk.
        """

    @classmethod
    def clean_disk_data(cls, filename, **kwargs):
        """Remove raw data from the disk.

        This method should not change any properties of any `Register`, which is why it is a classmethod and
        not an instancemethod. It merely removes the raw data.

        Most use-cases prefer the method `remove_disk_block`.

        :param filename: (type `pathlib.Path`) Where to remove the raw data. You may need to edit this
        filename if necessary, such as by adding a suffix, but you must return the edited filename.
        :raises Data_Not_Found_Error: If the data could not be cleaned because it doesn't exist.
        :return: (pathlib.Path) The exact path of the data removed.
        """

        if not filename.is_absolute():
            raise ValueError(NOT_ABSOLUTE_ERROR_MESSAGE.format(filename))

        try:
            filename.unlink(missing_ok = False)

        except FileNotFoundError as e:
            raise Data_Not_Found_Error(f"No file found at {str(filename)}") from e

        return filename

    def add_disk_block(self, blk, return_metadata = False, **kwargs):
        """Save a `Block` to disk and link it with this `Register`.

        :param blk: (type `Block`)
        :param return_metadata: (type `bool`, default `False`) Whether to return a `File_Metadata` object, which
        contains file creation date/time and size of dumped data to the disk.
        :raises Register_Error: If a duplicate `Block` already exists in this `Register`.
        """

        #DEBUG : 1, 2, 3, 4

        _FAIL_NO_RECOVER_ERROR_MESSAGE = "Could not successfully recover from a failed disk `Block` add!"

        self._check_open_raise("add_disk_block")

        self._check_readwrite_raise("add_disk_block")

        if not isinstance(blk, Block):
            raise TypeError("`blk` must be of type `Block`.")

        if not isinstance(return_metadata, bool):
            raise TypeError("`return_metadata` must be of type `bool`.")

        start_n_head = blk.start_n() // self._start_n_tail_mod

        if start_n_head != self._start_n_head :

            raise IndexError(
                "The `start_n` for the passed `Block` does not have the correct head:\n" +
                f"`tail_length`   : {self._start_n_tail_length}\n" +
                f"expected `head` : {self._start_n_head}\n"
                f"`start_n`       : {blk.start_n()}\n" +
                f"`start_n` head  : {start_n_head}\n" +
                "Please see the method `set_start_n_info` to troubleshoot this error."
            )

        apris = [apri for _, apri in blk.apri().iter_inner_info() if isinstance(apri, Apri_Info)]

        filename = None

        if _debug == 1:
            raise KeyboardInterrupt

        try:

            with self._db.begin(write = True) as rw_txn:

                with self._db.begin() as ro_txn:

                    # this will create ID's if necessary
                    for i, apri in enumerate(apris):
                        self._get_id_by_apri(apri, None, True, rw_txn)

                    blk_key = self._get_disk_block_key(

                        _BLK_KEY_PREFIX,
                        blk.apri(), None, blk.start_n(), len(blk),
                        False, rw_txn
                    )

                    if not lmdb_has_key(ro_txn, blk_key):

                        filename = random_unique_filename(self._local_dir, length=6)

                        if _debug == 2:
                            raise KeyboardInterrupt

                        filename = type(self).dump_disk_data(blk.segment(), filename, **kwargs)

                        if _debug == 3:
                            raise KeyboardInterrupt

                        filename_bytes = str(filename.name).encode("ASCII")
                        compressed_key = _COMPRESSED_KEY_PREFIX + blk_key[_BLK_KEY_PREFIX_LEN : ]

                        rw_txn.put(blk_key, filename_bytes)
                        rw_txn.put(compressed_key, _IS_NOT_COMPRESSED_VAL)

                        if len(blk) == 0:

                            warnings.warn(
                                "Added a length 0 disk `Block` to this `Register`.\n" +
                                f"`Register` message: {str(self)}\n" +
                                f"`Block`: {str(blk)}\n" +
                                f"`Register` location: {str(self._local_dir)}"
                            )

                        if return_metadata:
                            return File_Metadata.from_path(filename)

                    else:

                        raise Register_Error(
                            f"Duplicate `Block` with the following data already exists in this `Register`: " +
                            f"{str(blk.apri())}, start_n = {blk.start_n()}, length = {len(blk)}."
                        )

                if _debug == 4:
                    raise KeyboardInterrupt

        except BaseException as e:
            # We must assume that if an exception was thrown, `rw_txn` did not commit successfully.

            try:

                if filename is not None:
                    filename.unlink(missing_ok = True)

            except BaseException:
                raise Register_Recovery_Error(_FAIL_NO_RECOVER_ERROR_MESSAGE)

            else:

                if isinstance(e, lmdb.MapFullError):
                    raise Register_Error(_MEMORY_FULL_ERROR_MESSAGE.format(self._db_map_size)) from e

                else:
                    raise e

    def append_disk_block(self, blk, return_metadata = False, **kwargs):
        """Add a `Block` to disk and link it with this `Register`.

        If no disk `Block`s with the same `Apri_Info` as `blk` have previously been added to disk, then the `start_n`
        of `blk` will be set to 0. If not, then `start_n` will be set to one more than the largest index among all
        disk `Block`s with the same `Apri_Info` as `blk`.

        :param blk: (type `Block`)
        :param return_metadata: (type `bool`, default `False`) Whether to return a `File_Metadata` object, which
        contains file creation date/time and size of dumped data to the disk.
        :raises Register_Error: If a duplicate `Block` already exists in this `Register`.
        """

        if not isinstance(blk, Block):
            raise TypeError("`blk` must be of type `Block`.")

        if not isinstance(return_metadata, bool):
            raise TypeError("`return_metadata` must be of type `bool`.")

        if self.num_disk_blocks(blk.apri()) == 0:

            blk.set_start_n(0)
            self.add_disk_block(blk, **kwargs)

        else:

            start_n = 0

            for key, _ in self._iter_disk_block_pairs(_BLK_KEY_PREFIX, blk.apri(), None):

                _, _start_n, _length = self._convert_disk_block_key(_BLK_KEY_PREFIX_LEN, key)

                if start_n < _start_n + _length:
                    start_n = _start_n + _length

            blk.set_start_n(start_n)
            self.add_disk_block(blk, **kwargs)

    def remove_disk_block(self, apri, start_n = None, length = None, recursively = False, **kwargs):
        """Delete a disk `Block` and unlink it with this `Register`.

        :param apri: (type `Apri_Info`)
        :param start_n: (type `int`) Non-negative.
        :param length: (type `int`) Non-negative.
        :param recursively: (type `bool`)
        """

        # DEBUG : 1, 2, 3

        _FAIL_NO_RECOVER_ERROR_MESSAGE = "Could not successfully recover from a failed disk `Block` remove!"

        self._check_open_raise("remove_disk_block")

        self._check_readwrite_raise("remove_disk_block")

        start_n, length = Register._check_apri_start_n_length_raise(apri, start_n, length)

        start_n, length = self._resolve_start_n_length(apri, start_n, length)

        try:

            blk_key, compressed_key = self._check_blk_compressed_keys_raise(None, None, apri, None, start_n, length)

            if _debug == 1:
                raise KeyboardInterrupt

        except Data_Not_Found_Error:
            pass

        else:

            blk_filename, compressed_filename = self._check_blk_compressed_files_raise(
                blk_key, compressed_key, apri, start_n, length
            )

            if not is_deletable(blk_filename):
                raise OSError(f"Cannot delete `Block` file `{str(blk_filename)}`.")

            if compressed_filename is not None and not is_deletable(compressed_filename):
                raise OSError(f"Cannot delete compressed `Block` file `{str(compressed_filename)}`.")

            compressed_val = None
            blk_val = None

            try:

                with self._db.begin(write = True) as txn:

                    compressed_val = txn.get(compressed_key)
                    blk_val = txn.get(blk_key)
                    txn.delete(compressed_key)
                    txn.delete(blk_key)

                if _debug == 2:
                    raise KeyboardInterrupt

                if compressed_filename is not None:

                    blk_filename.unlink(missing_ok = False)

                    if _debug == 3:
                        raise KeyboardInterrupt

                    compressed_filename.unlink(missing_ok = False)

                else:
                    type(self).clean_disk_data(blk_filename, **kwargs)

            except BaseException as e:

                if blk_val is not None:

                    try:

                        if compressed_filename is not None:

                            if compressed_filename.exists():

                                blk_filename.touch(exist_ok = True)

                                with self._db.begin(write = True) as txn:

                                    txn.put(compressed_key, compressed_val)
                                    txn.put(blk_key, blk_val)

                            else:
                                raise Register_Recovery_Error(_FAIL_NO_RECOVER_ERROR_MESSAGE)

                        else:

                            if blk_filename.exists():

                                with self._db.begin(write = True) as txn:

                                    txn.put(compressed_key, compressed_val)
                                    txn.put(blk_key, blk_val)

                            else:
                                raise Register_Recovery_Error(_FAIL_NO_RECOVER_ERROR_MESSAGE)

                    except Register_Recovery_Error as ee:
                        raise ee

                    except BaseException:
                        raise Register_Recovery_Error(_FAIL_NO_RECOVER_ERROR_MESSAGE)

                if isinstance(e, lmdb.MapFullError):
                    raise Register_Error(_MEMORY_FULL_ERROR_MESSAGE.format(self._db_map_size)) from e

                else:
                    raise e

            return

        if recursively:

            for subreg in self._iter_subregisters():

                with subreg._recursive_open(False) as subreg:

                    try:
                        subreg.remove_disk_block(apri, start_n, length, True, **kwargs)

                    except Data_Not_Found_Error:
                        pass

                    else:
                        return

        raise Data_Not_Found_Error(
            _DISK_BLOCK_DATA_NOT_FOUND_ERROR_MSG_FULL.format(str(apri), start_n, length)
        )

    def disk_block(self, apri, start_n = None, length = None, return_metadata = False, recursively = False, **kwargs):

        self._check_open_raise("disk_block")

        start_n, length = Register._check_apri_start_n_length_raise(apri, start_n, length)

        if not isinstance(return_metadata, bool):
            raise TypeError("`return_metadata` must be of type `bool`.")

        if not isinstance(recursively, bool):
            raise TypeError("`recursively` must be of type `bool`.")

        start_n, length = self._resolve_start_n_length(apri, start_n, length)

        try:
            blk_key, compressed_key = self._check_blk_compressed_keys_raise(None, None, apri, None, start_n, length)

        except Data_Not_Found_Error:
            pass

        else:

            with self._db.begin() as txn:
                if txn.get(compressed_key) != _IS_NOT_COMPRESSED_VAL:
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
                with subreg._recursive_open(True) as subreg:
                    try:
                        return subreg.disk_block(apri, start_n, length, return_metadata, True)

                    except Data_Not_Found_Error:
                        pass

        raise Data_Not_Found_Error(
            _DISK_BLOCK_DATA_NOT_FOUND_ERROR_MSG_FULL.format(str(apri), start_n, length)
        )

    def disk_block_by_n(self, apri, n, return_metadata = False, recursively = False, **kwargs):

        self._check_open_raise("disk_block_by_n")

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
            for start_n, length in self.disk_intervals(apri):
                if start_n <= n < start_n + length:
                    return self.disk_block(apri, start_n, length, return_metadata, False, **kwargs)

        except Data_Not_Found_Error:
            pass

        if recursively:
            for subreg in self._iter_subregisters():
                with subreg._recursive_open(True) as subreg:
                    try:
                        return subreg.disk_block_by_n(apri, n, return_metadata, True, **kwargs)

                    except Data_Not_Found_Error:
                        pass

        raise Data_Not_Found_Error(_DISK_BLOCK_DATA_NOT_FOUND_ERROR_MSG_N.format(str(apri), n))

    def disk_blocks(self, apri, return_metadata = False, recursively = False, **kwargs):

        self._check_open_raise("disk_blocks")

        if not isinstance(apri, Apri_Info):
            raise TypeError("`apri` must be of type `Apri_Info`.")

        if not isinstance(return_metadata, bool):
            raise TypeError("`return_metadata` must be of type `bool`.")

        if not isinstance(recursively, bool):
            raise TypeError("`recursively` must be of type `bool`.")


        for start_n, length in self.disk_intervals(apri):
            try:
                yield self.disk_block(apri, start_n, length, return_metadata, False, **kwargs)

            except Data_Not_Found_Error:
                pass

        if recursively:
            for subreg in self._iter_subregisters():
                with subreg._recursive_open(True) as subreg:
                    for blk in subreg.disk_blocks(apri, return_metadata, True, **kwargs):
                        yield blk

    def disk_block_metadata(self, apri, start_n = None, length = None, recursively = False):

        self._check_open_raise("disk_block_metadata")

        start_n, length = Register._check_apri_start_n_length_raise(apri, start_n, length)

        if not isinstance(recursively, bool):
            raise TypeError("`recursively` must be of type `bool`.")

        start_n, length = self._resolve_start_n_length(apri, start_n, length)

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
                with subreg._recursive_open(True) as subreg:
                    try:
                        return subreg.disk_block_metadata(apri, start_n, length, True)

                    except Data_Not_Found_Error:
                        pass

        raise Data_Not_Found_Error(
            _DISK_BLOCK_DATA_NOT_FOUND_ERROR_MSG_FULL.format(str(apri), start_n, length)
        )

    def disk_intervals(self, apri):
        """Return a `list` of all tuples `(start_n, length)` associated to disk `Block`s.

        The tuples are sorted by increasing `start_n` and the larger `length` is used to break ties.

        :param apri: (type `Apri_Info`)
        :return: (type `list`)
        """

        self._check_open_raise("disk_intervals")

        if not isinstance(apri, Apri_Info):
            raise TypeError("`apri` must be of type `Apri_Info`.")

        return sorted([
            self._convert_disk_block_key(_BLK_KEY_PREFIX_LEN, key, apri)[1:]
            for key, _ in self._iter_disk_block_pairs(_BLK_KEY_PREFIX, apri, None)
        ], key = lambda t: (t[0], -t[1]))

    def num_disk_blocks(self, apri):

        self._check_open_raise("num_disk_blocks")

        try:

            return lmdb_count_keys(
                self._db,
                _BLK_KEY_PREFIX + self._get_id_by_apri(apri, None, False) + _KEY_SEP
            )

        except Data_Not_Found_Error:
            return 0

    def compress(self, apri, start_n = None, length = None, compression_level = 6, return_metadata = False):
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

        # DEBUG : 1, 2, 3, 4

        _FAIL_NO_RECOVER_ERROR_MESSAGE = "Could not recover successfully from a failed disk `Block` compress!"

        self._check_open_raise("compress")

        self._check_readwrite_raise("compress")

        start_n, length = Register._check_apri_start_n_length_raise(apri, start_n, length)

        if not is_int(compression_level):
            raise TypeError("`compression_level` must be of type `int`.")
        else:
            compression_level = int(compression_level)

        if not isinstance(return_metadata, bool):
            raise TypeError("`return_metadata` must be of type `bool`.")

        if not (0 <= compression_level <= 9):
            raise ValueError("`compression_level` must be between 0 and 9.")

        start_n, length = self._resolve_start_n_length(apri, start_n, length)

        compressed_key = self._get_disk_block_key(
            _COMPRESSED_KEY_PREFIX, apri, None, start_n, length, False
        )

        blk_key, compressed_key = self._check_blk_compressed_keys_raise(
            None, compressed_key, apri, None, start_n, length
        )

        with self._db.begin() as txn:
            compressed_val = txn.get(compressed_key)

        if compressed_val != _IS_NOT_COMPRESSED_VAL:

            raise Compression_Error(
                "The disk `Block` with the following data has already been compressed: " +
                f"{str(apri)}, start_n = {start_n}, length = {length}"
            )

        with self._db.begin() as txn:
            blk_filename = self._local_dir / txn.get(blk_key).decode("ASCII")

        compressed_filename = random_unique_filename(self._local_dir, COMPRESSED_FILE_SUFFIX)
        compressed_val = compressed_filename.name.encode("ASCII")

        cleaned = False

        if _debug == 1:
            raise KeyboardInterrupt

        try:

            with self._db.begin(write = True) as txn:

                txn.put(compressed_key, compressed_val)

                if _debug == 2:
                    raise KeyboardInterrupt

            with zipfile.ZipFile(

                compressed_filename,  # target filename
                "x",  # zip mode (write, but don't overwrite)
                zipfile.ZIP_DEFLATED,  # compression mode
                True,  # use zip64
                compression_level,
                strict_timestamps=False  # change timestamps of old or new files

            ) as compressed_fh:

                compressed_fh.write(blk_filename, blk_filename.name)

                if _debug == 3:
                    raise KeyboardInterrupt

            if _debug == 4:
                raise KeyboardInterrupt

            type(self).clean_disk_data(blk_filename)
            cleaned = True
            blk_filename.touch(exist_ok = False)

        except BaseException as e:

            try:

                with self._db.begin(write = True) as txn:
                    txn.put(compressed_key, _IS_NOT_COMPRESSED_VAL)

                if cleaned or not blk_filename.exists():
                    raise Register_Recovery_Error(_FAIL_NO_RECOVER_ERROR_MESSAGE)

                else:
                    compressed_filename.unlink(missing_ok = True)

            except Register_Recovery_Error as ee:
                raise ee

            except BaseException:
                raise Register_Recovery_Error(_FAIL_NO_RECOVER_ERROR_MESSAGE)

            else:

                if isinstance(e, lmdb.MapFullError):
                    raise Register_Error(_MEMORY_FULL_ERROR_MESSAGE.format(self._db_map_size)) from e

                else:
                    raise e

        if return_metadata:
            return File_Metadata.from_path(compressed_filename)

        else:
            return None

    def decompress(self, apri, start_n = None, length = None, return_metadata = False):
        """Decompress a `Block`.

        :param apri: (type `Apri_Info`)
        :param start_n: (type `int`) Non-negative.
        :param length: (type `int`) Non-negative.
        :param return_metadata: (type `bool`, default `False`) Whether to return a `File_Metadata` object that
        describes the decompressed file.
        :raise Decompression_Error: If the `Block` is not compressed.
        :return: (type `list`) If `return_metadata is True`.
        """

        # DEBUG : 1, 2, 3, 4

        _FAIL_NO_RECOVER_ERROR_MESSAGE = "Could not recover successfully from a failed disk `Block` decompress!"

        self._check_open_raise("decompress")

        self._check_readwrite_raise("decompress")

        start_n, length = Register._check_apri_start_n_length_raise(apri, start_n, length)

        if not isinstance(return_metadata, bool):
            raise TypeError("`return_metadata` must be of type `bool`.")

        start_n, length = self._resolve_start_n_length(apri, start_n, length)

        blk_key, compressed_key = self._check_blk_compressed_keys_raise(None, None, apri, None, start_n, length)

        with self._db.begin() as txn:
            compressed_val = txn.get(compressed_key)

        if compressed_val == _IS_NOT_COMPRESSED_VAL:

            raise Decompression_Error(
                "The disk `Block` with the following data is not compressed: " +
                f"{str(apri)}, start_n = {start_n}, length = {length}"
            )

        with self._db.begin() as txn:
            blk_filename = txn.get(blk_key).decode("ASCII")

        blk_filename = self._local_dir / blk_filename
        compressed_filename = self._local_dir / compressed_val.decode("ASCII")
        deleted = False

        if not is_deletable(blk_filename):
            raise OSError(f"Cannot delete ghost file `{str(blk_filename)}`.")

        if not is_deletable(compressed_filename):
            raise OSError(f"Cannot delete compressed file `{str(compressed_filename)}`.")

        if _debug == 1:
            raise KeyboardInterrupt

        try:

            with self._db.begin(write = True) as txn:

                # delete ghost file
                blk_filename.unlink(missing_ok = False)
                deleted = True

                if _debug == 2:
                    raise KeyboardInterrupt

                with zipfile.ZipFile(compressed_filename, "r") as compressed_fh:

                    compressed_fh.extract(blk_filename.name, self._local_dir)

                    if _debug == 3:
                        raise KeyboardInterrupt

                txn.put(compressed_key, _IS_NOT_COMPRESSED_VAL)

                if _debug == 4:
                    raise KeyboardInterrupt

                compressed_filename.unlink(missing_ok = False)

        except BaseException as e:

            try:

                if not compressed_filename.is_file():
                    raise Register_Recovery_Error(_FAIL_NO_RECOVER_ERROR_MESSAGE)

                elif deleted or not blk_filename.is_file():

                    blk_filename.unlink(missing_ok = True)
                    blk_filename.touch(exist_ok = False)

            except Register_Recovery_Error as ee:
                raise ee

            except BaseException:
                raise Register_Recovery_Error(_FAIL_NO_RECOVER_ERROR_MESSAGE)

            else:

                if isinstance(e, lmdb.MapFullError):
                    raise Register_Error(_MEMORY_FULL_ERROR_MESSAGE.format(self._db_map_size)) from e

                else:
                    raise e

        if return_metadata:
            return File_Metadata.from_path(blk_filename)

        else:
            return None

    def set_hashing(self, apri, hashing):
        """Enable or disable automatic hashing for disk `Block`s with `apri`.

        If `hashing` is set to `True`, then every disk `Block` with `apri` added to this `Register` will be hashed. A
        `Block` is hashed by calling `hash` on each of its entries. The hashes are saved to a hash-set on disk.

        If `hashing` is set to `False`, then all hashes associated to `apri` will be DELETED (if they exist) and no
        future hashes are calculated.

        For best results, set hashing to `True` only before adding any disk `Block`s with `apri`.

        :param hashing: (type `bool`)
        """

    #################################
    #    PROTEC DISK BLK METHODS    #

    def _get_disk_block_key(self, prefix, apri, apri_json, start_n, length, missing_ok, txn = None):
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
        :param txn: (type `lmbd.Transaction`, default `None`) The transaction to query. If `None`, then use open a new
        transaction and commit it after this method resolves.
        :raises Apri_Info_Not_Found_Error: If `missing_ok is False` and `apri` is not known to this `Register`.
        :return: (type `bytes`)
        """

        if apri is None and apri_json is None:
            raise ValueError

        _id = self._get_id_by_apri(apri, apri_json, missing_ok, txn)
        tail = start_n % self._start_n_tail_mod

        return (
                prefix                      +
                _id                         + _KEY_SEP +
                str(tail)  .encode("ASCII") + _KEY_SEP +
                str(length).encode("ASCII")
        )

    def _iter_disk_block_pairs(self, prefix, apri, apri_json, txn = None):
        """Iterate over key-value pairs for block entries.

        :param prefix: (type `bytes`)
        :param apri: (type `Apri_Info`)
        :param apri_json: (type `bytes`)
        :param txn: (type `lmbd.Transaction`, default `None`) The transaction to query. If `None`, then use open a new
        transaction and commit it after this method resolves.
        :raise Data_Not_Found_Error: If `apri` is not a disk `Apri_Info`.
        :return: (type `bytes`) key
        :return: (type `bytes`) val
        """


        if apri_json is not None or apri is not None:

            prefix += self._get_id_by_apri(apri, apri_json, False, txn)
            prefix += _KEY_SEP

        if txn is None:
            txn = self._db

        with lmdb_prefix_iterator(txn, prefix) as it:
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

    def _convert_disk_block_key(self, prefix_len, key, apri = None, txn = None):
        """
        :param prefix_len: (type `int`) Positive.
        :param key: (type `bytes`)
        :param apri: (type `Apri_Info`, default None) If `None`, the relevant `apri` is acquired through a database
        query.
        :param txn: (type `lmbd.Transaction`, default `None`) The transaction to query. If `None`, then use open a new
        transaction and commit it after this method resolves.
        :return: (type `Apri_Info`)
        :return (type `int`) start_n
        :return (type `int`) length, non-negative
        """

        apri_id, start_n_bytes, length_bytes = Register._split_disk_block_key(prefix_len, key)

        if apri is None:

            apri_json = self._get_apri_json_by_id(apri_id, txn)
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

        if not lmdb_has_key(self._db, blk_key) or not lmdb_has_key(self._db, compressed_key):
            raise Data_Not_Found_Error(
                _DISK_BLOCK_DATA_NOT_FOUND_ERROR_MSG_FULL.format(apri, start_n, length)
            )

        return blk_key, compressed_key

    def _check_blk_compressed_files_raise(self, blk_key, compressed_key, apri, start_n, length):

        with self._db.begin() as txn:
            blk_val = txn.get(blk_key)
            compressed_val = txn.get(compressed_key)

        blk_filename = self._local_dir / blk_val.decode("ASCII")

        if compressed_val != _IS_NOT_COMPRESSED_VAL:
            compressed_filename = self._local_dir / compressed_val.decode("ASCII")

            if not compressed_filename.exists() or not blk_filename.exists():
                raise Data_Not_Found_Error(
                    _DISK_BLOCK_DATA_NOT_FOUND_ERROR_MSG_FULL.format(str(apri), start_n, length)
                )

            return blk_filename, compressed_filename

        else:

            if not blk_filename.exists():
                raise Data_Not_Found_Error(
                    _DISK_BLOCK_DATA_NOT_FOUND_ERROR_MSG_FULL.format(str(apri), start_n, length)
                )

            return blk_filename, None

    @staticmethod
    def _check_apri_start_n_length_raise(apri, start_n, length):

        if not isinstance(apri, Apri_Info):
            raise TypeError("`apri` must be of type `Apri_Info`")

        if not is_int(start_n) and start_n is not None:
            raise TypeError("start_n` must be an `int`")

        elif start_n is not None:
            start_n = int(start_n)

        if not is_int(length) and length is not None:
            raise TypeError("`length` must be an `int`")

        elif length is not None:
            length = int(length)

        if start_n is not None and start_n < 0:
            raise ValueError("`start_n` must be non-negative")

        if length is not None and length < 0:
            raise ValueError("`length` must be non-negative")

        return start_n, length

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

    def _resolve_start_n_length(self, apri, start_n, length):
        """
        :param apri: (type `Apri_Info`)
        :param start_n: (type `int` or `NoneType`) Non-negative.
        :param length: (type `int` or `NoneType`) Positive.
        :raise Data_Not_Found_Error
        :raise ValueError: If `start_n is None and length is not None`.
        :return: (type `int`) Resolved `start_n`, always `int`.
        :return: (type `int`) Resolved `length`, always `length`.
        """

        if start_n is not None and length is not None:
            return start_n, length

        elif start_n is not None and length is None:

            key = self._get_disk_block_key(_BLK_KEY_PREFIX, apri, None, start_n, 1, False)

            first_key_sep_index = key.find(_KEY_SEP)
            second_key_sep_index = key.find(_KEY_SEP, first_key_sep_index + 1)

            prefix = key [ : second_key_sep_index + 1]

            i = -1
            largest_length = None
            key_with_largest_length = None
            with lmdb_prefix_iterator(self._db, prefix) as it:

                for i, (key, _) in enumerate(it):

                    length = int(Register._split_disk_block_key(_BLK_KEY_PREFIX_LEN, key)[2].decode("ASCII"))

                    if largest_length is None or length > largest_length:

                        largest_length = length
                        key_with_largest_length = key

            if i == -1:
                raise Data_Not_Found_Error(f"No disk `Block`s found with {str(apri)} and start_n = {start_n}.")

            else:
                return self._convert_disk_block_key(_BLK_KEY_PREFIX_LEN, key_with_largest_length, apri)[1:]

        elif start_n is None and length is None:

            prefix = _BLK_KEY_PREFIX + self._get_id_by_apri(apri, None, False) + _KEY_SEP

            smallest_start_n = None
            i = -1
            with lmdb_prefix_iterator(self._db, prefix) as it:

                for i, (key, _) in enumerate(it):

                    start_n = int(Register._split_disk_block_key(_BLK_KEY_PREFIX_LEN, key)[1].decode("ASCII"))

                    if smallest_start_n is None or start_n < smallest_start_n:
                        smallest_start_n = start_n

            if i == -1:
                raise Data_Not_Found_Error(f"No disk `Block`s found with {str(apri)}.")

            else:
                return self._resolve_start_n_length(apri, smallest_start_n, None)

        else:
            raise ValueError(f"If you specify a `Block` length, you must also specify a `start_n`.")

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

    def ram_block_by_n(self, apri, n, recursively = False):

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
            start_n = blk.start_n()
            if blk.apri() == apri and start_n <= n < start_n + len(blk):
                return blk

        if recursively:
            self._check_open_raise("ram_block_by_n")
            for subreg in self._iter_subregisters():
                with subreg._recursive_open(True) as subreg:
                    try:
                        return subreg.disk_block_by_n(apri, n, True)
                    except Data_Not_Found_Error:
                        pass

        raise Data_Not_Found_Error(
            _RAM_BLOCK_DATA_NOT_FOUND_ERROR_MSG_N.format(str(apri), n)
        )

    def ram_blocks(self, apri, recursively = False):

        if not isinstance(apri, Apri_Info):
            raise TypeError("`apri` must be of type `Apri_Info`.")

        if not isinstance(recursively, bool):
            raise TypeError("`recursively` must be of type `bool`.")

        for blk in self._ram_blks:
            if blk.apri() == apri:
                yield blk

        if recursively:
            self._check_open_raise("ram_blocks")
            for subreg in self._iter_subregisters():
                with subreg._recursive_open(True) as subreg:
                    for blk in subreg.ram_blocks(apri, True):
                        yield blk

    #################################
    #    PROTEC RAM BLK METHODS     #

    #################################
    # PUBLIC RAM & DISK BLK METHODS #

    def __getitem__(self, apri_and_n_and_recursively):

        if not isinstance(apri_and_n_and_recursively, tuple) or len(apri_and_n_and_recursively) <= 1:
            raise TypeError("Must pass at least two arguments to `reg[]`.")

        if len(apri_and_n_and_recursively) >= 4:
            raise TypeError("Must pass at most three arguments to `reg[]`.")

        if len(apri_and_n_and_recursively) == 2:

            apri, n = apri_and_n_and_recursively
            recursively = False

        else:
            apri, n, recursively = apri_and_n_and_recursively

        return self.get(apri, n, recursively)

    def get(self, apri, n, recursively = False, **kwargs):

        if not isinstance(apri, Apri_Info):
            raise TypeError("The first argument to `reg[]` must be an `Apri_Info.")

        if not is_int(n) and not isinstance(n, slice):
            raise TypeError("The second argument to `reg[]` must be an `int` or a `slice`.")

        elif is_int(n):
            n = int(n)

        else:

            _n = [None]*3

            if n.start is not None and not is_int(n.start):
                raise TypeError("Start index of slice must be an `int`.")

            elif n.start is not None:
                _n[0] = int(n.start)

            if n.stop is not None and not is_int(n.stop):
                raise TypeError("Stop index of slice must be an `int`.")

            elif n.stop is not None:
                _n[1] = int(n.stop)

            if n.step is not None and not is_int(n.step):
                raise TypeError("Step index of slice must be an `int`.")

            elif n.step is not None:
                _n[2] = int(n.stop)

            n = slice(*tuple(_n))

        if not isinstance(recursively, bool):
            raise TypeError("The third argument of `reg[]` must be of type `bool`.")

        if isinstance(n, slice):

            if n.start is not None and n.start < 0:
                raise ValueError("Start index cannot be negative.")

            if n.stop is not None and n.stop < 0:
                raise ValueError("Stop index cannot be negative.")

        if isinstance(n, slice):
            # return iterator if given slice
            return _Element_Iter(self, apri, n, recursively, kwargs)

        else:

            try:
                return self.ram_block_by_n(apri, n)[n]

            except Data_Not_Found_Error:

                try:
                    return self.disk_block_by_n(apri, n, **kwargs)[n]

                except Data_Not_Found_Error:
                    pass

            raise Data_Not_Found_Error(_DISK_RAM_BLOCK_DATA_NOT_FOUND_ERROR_MSG_N.format(str(apri), n))

    def intervals(self, apri, combine = True, recursively = False):

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

    def total_length(self, apri, recursively = False):

        self._check_open_raise("total_length")

        if apri in self:
            return sum(length for _, length in self.intervals(apri, True, recursively))

        else:
            return 0

    #################################
    # PROTEC RAM & DISK BLK METHODS #

    def _iter_converted_ram_and_disk_block_datas(self, apri, recursively = False):

        for blk in self._ram_blks:
            if blk.apri() == apri:
                yield apri, blk.start_n(), len(blk)

        try:
            self._get_id_by_apri(apri, None, False)

        except Data_Not_Found_Error:
            pass

        else:

            for key, _ in self._iter_disk_block_pairs(_BLK_KEY_PREFIX, apri, None):
                yield self._convert_disk_block_key(_BLK_KEY_PREFIX_LEN, key, apri)

        if recursively:
            for subreg in self._iter_subregisters():
                with subreg._recursive_open(True) as subreg:
                    for data in subreg._iter_ram_and_disk_block_datas(apri, True):
                        yield data

class Pickle_Register(Register):

    @classmethod
    def dump_disk_data(cls, data, filename, **kwargs):

        if len(kwargs) > 0:
            raise KeyError("`Pickle_Register.add_disk_block` accepts no keyword-arguments.")

        filename = filename.with_suffix(".pkl")

        with filename.open("wb") as fh:
            pickle.dump(data, fh)

        return filename

    @classmethod
    def load_disk_data(cls, filename, **kwargs):

        if len(kwargs) > 0:
            raise KeyError("`Pickle_Register.disk_block` accepts no keyword-arguments.")

        with filename.open("rb") as fh:
            return pickle.load(fh), filename

    @classmethod
    def clean_disk_data(cls, filename, **kwargs):
        pass

Register.add_subclass(Pickle_Register)

class Numpy_Register(Register):

    @classmethod
    def dump_disk_data(cls, data, filename, **kwargs):

        if len(kwargs) > 0:
            raise KeyError("This method accepts no keyword-arguments.")

        filename = filename.with_suffix(".npy")
        np.save(filename, data, allow_pickle = False, fix_imports = False)
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
                "The keyword-argument `mmap_mode` for `Numpy_Register.disk_block` can only have the values " +
                "`None`, 'r+', 'r', 'w+', 'c'. Please see " +
                "https://numpy.org/doc/stable/reference/generated/numpy.memmap.html#numpy.memmap for more information."
            )

        return np.load(filename, mmap_mode = mmap_mode, allow_pickle = False, fix_imports = False), filename

    @classmethod
    def clean_disk_data(cls, filename, **kwargs):

        if len(kwargs) > 0:
            raise KeyError("This method accepts no keyword-arguments.")

        filename = filename.with_suffix(".npy")
        return Register.clean_disk_data(filename)

    def disk_block(self, apri, start_n = None, length = None, return_metadata = False, recursively = False, **kwargs):
        """
        :param apri: (type `Apri_Info`)
        :param start_n: (type `int`)
        :param length: (type `length`) non-negative
        :param return_metadata: (type `bool`, default `False`) Whether to return a `File_Metadata` object, which
        contains file creation date/time and size of dumped saved on the disk.
        :param recursively: (type `bool`, default `False`) Search all subregisters for the `Block`.
        :param mmap_mode: (type `str`, optional) Load the Numpy file using memory mapping, see
        https://numpy.org/doc/stable/reference/generated/numpy.memmap.html#numpy.memmap for more information.
        :return: (type `File_Metadata`) If `return_metadata is True`.
        """

        ret = super().disk_block(apri, start_n, length, return_metadata, recursively, **kwargs)

        if return_metadata:
            blk = ret[0]

        else:
            blk = ret

        if isinstance(blk.segment(), np.memmap):
            blk = Memmap_Block(blk.segment(), blk.apri(), blk.start_n())

        if return_metadata:
            return blk, ret[1]

        else:
            return blk

    def concatenate_disk_blocks(self, apri, start_n = None, length = None, delete = False, return_metadata = False):
        """Concatenate several `Block`s into a single `Block` along axis 0 and save the new one to the disk.

        If `delete = True`, then the smaller `Block`s are deleted automatically.

        The interval `range(start_n, start_n + length)` must be the disjoint union of intervals of the form
        `range(blk.start_n(), blk.start_n() + len(blk))`, where `blk` is a disk `Block` with `Apri_Info`
        given by `apri`.

        Length-0 `Block`s are ignored.

        If `start_n` is not specified, it is taken to be the smallest `start_n` of any `Block` saved to this
        `Register`. If `length` is not specified, it is taken to be the length of the largest
        contiguous set of indices that start with `start_n`. If `start_n` is not specified but `length` is, a
        ValueError is raised.

        :param apri: (type `Apri_Info`)
        :param start_n: (type `int`) Non-negative.
        :param length: (type `int`) Positive.
        :param delete: (type `bool`, default `False`)
        :param return_metadata: (type `bool`, default `False`) Whether to return a `File_Metadata` object, which
        contains file creation date/time and size of dumped dumped to the disk.
        :raise Data_Not_Found_Error: If the union of the intervals of relevant disk `Block`s does not equal
        `range(start_n, start_n + length)`.
        :raise ValueError: If any two intervals of relevant disk `Block`s intersect.
        :raise ValueError: If any two relevant disk `Block` segments have inequal shapes.
        :return: (type `File_Metadata`) If `return_metadata is True`.
        """

        _FAIL_NO_RECOVER_ERROR_MESSAGE = "Could not successfully recover from a failed disk `Block` concatenation!"

        self._check_open_raise("concatenate_disk_blocks")

        self._check_readwrite_raise("concatenate_disk_blocks")

        start_n, length = Register._check_apri_start_n_length_raise(apri, start_n, length)

        if not isinstance(return_metadata, bool):
            raise TypeError("`return_metadata` must be of type `bool`.")

        # infer start_n
        start_n, _ = self._resolve_start_n_length(apri, start_n, length)

        # this implementation depends on `disk_intervals` returning smaller start_n before larger
        # ones and, when ties occur, larger lengths before smaller ones.

        if length is None:
            # infer length

            current_segment = False
            length = 0

            for _start_n, _length in self.disk_intervals(apri):

                if _length > 0:

                    if current_segment:

                        if start_n > _start_n:
                            raise RuntimeError("Could not infer a value for `length`.")

                        elif start_n == _start_n:
                            raise ValueError(
                                f"Overlapping `Block` intervals found with {str(apri)}."
                            )

                        else:

                            if start_n + length > _start_n:
                                raise ValueError(
                                    f"Overlapping `Block` intervals found with {str(apri)}."
                                )

                            elif start_n + length == _start_n:
                                length += _length

                            else:
                                break

                    else:

                        if start_n < _start_n:
                            raise Data_Not_Found_Error(
                                f"No disk `Block` found with the following data: {str(apri)}, start_n = {start_n}."
                            )

                        elif start_n == _start_n:

                            length += _length
                            current_segment = True

            if length == 0:
                raise RuntimeError("could not infer a value for `length`.")

            warnings.warn(f"`length` value not specified, inferred value: `length = {length}`.")

        combined_interval = None

        last_check = False
        last__start_n = None

        intervals_to_get = []

        for _start_n, _length in self.disk_intervals(apri):
            # infer blocks to combine

            if last_check:

                if last__start_n == _start_n and _length > 0:
                    raise ValueError(f"Overlapping `Block` intervals found with {str(apri)}.")

                else:
                    break

            if _length > 0:

                last__start_n = _start_n

                if _start_n < start_n:

                    if start_n < _start_n + _length:
                        raise ValueError(
                            f"The first `Block` is too long. Try again by calling `reg.concatenate_disk_blocks({str(apri)}, " +
                            f"{_start_n}, {length - (_start_n - start_n)})`."
                        )

                else:

                    if combined_interval is None:

                        if _start_n > start_n:

                            raise Data_Not_Found_Error(
                                f"No disk `Block` found with the following data: `{str(apri)}, start_n = {start_n}`."
                            )

                        elif _start_n == start_n:

                            combined_interval = (_start_n, _length)
                            intervals_to_get.append((_start_n, _length))
                            last_check = _start_n + _length == start_n + length

                        else:
                            raise RuntimeError("Something went wrong trying to combine `Block`s.")

                    else:

                        if _start_n > sum(combined_interval):

                            raise Data_Not_Found_Error(
                                f"No `Block` found covering indices {sum(combined_interval)} through "
                                f"{_start_n-1} (inclusive) with {str(apri)}."
                            )

                        elif _start_n == sum(combined_interval):

                            if _start_n + _length > start_n + length:
                                raise ValueError(
                                    f"The last `Block` is too long. Try again by calling `reg.concatenate_disk_blocks({str(apri)}, " +
                                    f"{start_n}, {length - (_start_n + _length - (start_n + length))})`."
                                )

                            combined_interval = (start_n, combined_interval[1] + _length)
                            intervals_to_get.append((_start_n, _length))
                            last_check = _start_n + _length == start_n + length

                        else:
                            raise ValueError(f"Overlapping `Block` intervals found with {str(apri)}.")


        if len(intervals_to_get) == 1:

            if return_metadata:
                return self.disk_block_metadata(apri, *intervals_to_get)

            else:
                return None

        blks = []
        fixed_shape = None
        ref_blk = None
        failure_reinsert_indices = []
        combined_blk = None

        try:

            for _start_n, _length in intervals_to_get:
                # check that blocks have the correct shape

                blk = self.disk_block(apri, _start_n, _length, False, False, mmap_mode = "r")
                blks.append(blk)

                if fixed_shape is None:

                    fixed_shape = blk.segment().shape[1:]
                    ref_blk = blk

                elif fixed_shape != blk.segment().shape[1:]:

                    raise ValueError(
                        "Cannot combine the following two `Block`s because all axes other than axis 0 must have the same " +
                        "shape:\n" +
                        f"{str(apri)}, start_n = {ref_blk.start_n()}, length = {len(ref_blk)}\n, shape = " +
                        f"{str(fixed_shape)}\n" +
                        f"{str(apri)}, start_n = {_start_n}, length = {_length}\n, shape = " +
                        f"{str(blk.segment().shape)}\n"

                    )

            combined_blk = np.concatenate([blk.segment() for blk in blks], axis=0)
            combined_blk = Block(combined_blk, apri, start_n)
            ret = self.add_disk_block(combined_blk, return_metadata)

            if _debug == 1:
                raise KeyboardInterrupt

            if delete:

                for blk in blks:

                    _start_n = blk.start_n()
                    _length = len(blk)
                    blk.close()
                    self.remove_disk_block(apri, _start_n, _length, False)
                    failure_reinsert_indices.append((_start_n, _length))

                    if _debug == 2:
                        raise KeyboardInterrupt

        except BaseException as e:

            try:

                if combined_blk is not None and isinstance(combined_blk, Block) and delete:

                    for _start_n, _length in failure_reinsert_indices:
                        self.add_disk_block(combined_blk[_start_n: _start_n + _length])

            except BaseException:
                raise Register_Recovery_Error(_FAIL_NO_RECOVER_ERROR_MESSAGE)

            else:
                raise e

        finally:

            for blk in blks:
                blk.close()

        return ret

Register.add_subclass(Numpy_Register)

class HDF5_Register(Register):

    @classmethod
    def dump_disk_data(cls, data, filename, **kwargs):
        pass

    @classmethod
    def load_disk_data(cls, filename, **kwargs):
        pass

    @classmethod
    def clean_disk_data(cls, filename, **kwargs):
        pass

Register.add_subclass(HDF5_Register)

class _Element_Iter:

    def __init__(self, reg, apri, slc, recursively, kwargs):

        self.reg = reg
        self.apri = apri
        self.step = slc.step if slc.step else 1
        self.stop = slc.stop
        self.recursively = recursively
        self.kwargs = kwargs
        self.curr_blk = None
        self.intervals = None
        self.curr_n = slc.start if slc.start else 0

    def update_sequences_calculated(self):
        self.intervals = dict(self.reg.intervals(self.apri, False, self.recursively))

    def get_next_block(self):

        try:
            return self.reg.ram_block_by_n(self.apri, self.curr_n, self.recursively)

        except Data_Not_Found_Error:
            return self.reg.disk_block_by_n(self.apri, self.curr_n, self.recursively, **self.kwargs)

    def __iter__(self):
        return self

    def __next__(self):

        if self.stop is not None and self.curr_n >= self.stop:
            raise StopIteration

        elif self.curr_blk is None:

            self.intervals = self.reg.intervals(self.apri, False, self.recursively)
            self.curr_n = max( self.intervals[0][0] , self.curr_n )

            try:
                self.curr_blk = self.get_next_block()

            except Data_Not_Found_Error:
                raise StopIteration

        elif self.curr_n not in self.curr_blk:

            try:
                self.curr_blk = self.get_next_block()

            except Data_Not_Found_Error:

                self.intervals = self.reg.intervals(self.apri, False, self.recursively)

                for start_n, length in self.intervals:

                    if start_n > self.curr_n:

                        self.curr_n += math.ceil( (start_n - self.curr_n) / self.step ) * self.step
                        break

                else:
                    raise StopIteration

                self.curr_blk = self.get_next_block()

        ret = self.curr_blk[self.curr_n]
        self.curr_n += self.step
        return ret