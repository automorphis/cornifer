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

import math
import pickle
import shutil
import warnings
import zipfile
from contextlib import contextmanager, ExitStack
from pathlib import Path
from abc import ABC, abstractmethod

import lmdb
import numpy as np

from .errors import DataNotFoundError, RegisterAlreadyOpenError, RegisterError, CompressionError, \
    DecompressionError, NOT_ABSOLUTE_ERROR_MESSAGE, RegisterRecoveryError
from .info import ApriInfo, AposInfo, _InfoJsonEncoder, _InfoJsonDecoder, _Info
from .blocks import Block, MemmapBlock, ReleaseBlock
from .filemetadata import FileMetadata
from ._utilities import random_unique_filename, is_int, resolve_path, BYTES_PER_MB, is_deletable, check_type, \
    check_return_int_None_default, check_Path, check_return_int, bytify_num, intify_bytes, intervals_overlap, \
    write_txt_file, read_txt_file, intervals_subset
from ._utilities.lmdb import lmdb_has_key, lmdb_prefix_iter, open_lmdb, lmdb_count_keys, \
    ReversibleTransaction, is_transaction, lmdb_prefix_list
from .regfilestructure import VERSION_FILEPATH, LOCAL_DIR_CHARS, \
    COMPRESSED_FILE_SUFFIX, MSG_FILEPATH, CLS_FILEPATH, check_reg_structure, DATABASE_FILEPATH, \
    REG_FILENAME, MAP_SIZE_FILEPATH, SHORTHAND_FILEPATH
from .version import CURRENT_VERSION, COMPATIBLE_VERSIONS

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
_LENGTH_LENGTH_KEY         = b"lenlen"

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

def _blk_not_found_err_msg(diskonly, apri, n = None, startn = None, length = None):

    if (startn is not None or length is not None) and n is not None:
        raise ValueError

    if startn is None and length is not None:
        raise ValueError

    if diskonly:
        type_ = "disk"

    else:
        type_ = "disk nor RAM"

    if n is not None:
        return f"No {type_} `Block` found with the following data: {str(apri)}, n = {n}."

    elif startn is not None and length is None:
        return f"No {type_} `Block` found with the following data: {str(apri)}, startn = {startn}."

    elif startn is not None and length is not None:
        return f"No {type_} `Block` found with the following data: {str(apri)}, startn = {startn}, length = {length}."

    else:
        return f"No {type_} `Block` found with the following data: {str(apri)}."

_NOT_CREATED_ERROR_MESSAGE = (
    "The `Register` database has not been created. You must do `with reg.open() as reg:` at least once before " +
    "calling the method `{0}`."
)
_MEMORY_FULL_ERROR_MESSAGE = (
    "Exceeded max `Register` size of {0} Bytes. Please increase the max size using the method `increase_reg_size`."
)
_REG_ALREADY_ADDED_ERROR_MESSAGE = "Already added as subregister."
_NO_APRI_ERROR_MESSAGE = "The following `ApriInfo` is not known to this `Register` : {0}"

#################################
#           CONSTANTS           #

_START_N_TAIL_LENGTH_DEFAULT   = 12
_LENGTH_LENGTH_DEFAULT         = 7
_MAX_LENGTH_DEFAULT            = 10 ** _LENGTH_LENGTH_DEFAULT - 1
_START_N_HEAD_DEFAULT          = 0
_INITIAL_REGISTER_SIZE_DEFAULT = 5 * BYTES_PER_MB
_MAX_NUM_APRI_LENGTH           = 6
_MAX_NUM_APRI                  = 10**_MAX_NUM_APRI_LENGTH

class Register(ABC):

    #################################
    #     PUBLIC INITIALIZATION     #

    def __init__(self, saves_dir, shorthand, msg, initial_reg_size = None):
        """
        :param saves_dir: (type `str`) Directory where this `Register` is saved.
        :param shorthand: (type `str`) A word or short phrase describing this `Register`.
        :param msg: (type `str`) A more detailed message describing this `Register`.
        :param initial_reg_size: (type `int`, default 5) Size in bytes. You may wish to set this lower
        than 5 MB if you do not expect to add many disk `Block`s to your register and you are concerned about disk
        memory. If your `Register` exceeds `initial_register_size`, then you can adjust the database size later via the
        method `increase_reg_size`. If you are on a non-Windows system, there is no harm in setting this value
        to be very large (e.g. 1 TB).
        """

        check_Path(saves_dir, "saves_dir")
        check_type(shorthand, "shorthand", str)
        check_type(msg, "msg", str)
        initial_reg_size = check_return_int_None_default(
            initial_reg_size, "initial_reg_size", _INITIAL_REGISTER_SIZE_DEFAULT
        )

        if initial_reg_size <= 0:
            raise ValueError("`initial_reg_size` must be positive.")

        self.saves_dir = resolve_path(Path(saves_dir))

        if not self.saves_dir.is_dir():
            raise FileNotFoundError(
                f"You must create the file `{str(self.saves_dir)}` before calling " +
                f"`{self.__class__.__name__}(\"{str(self.saves_dir)}\", \"{msg}\")`."
            )

        self._shorthand = shorthand
        self._shorthand_filepath = None
        self._msg = msg
        self._msg_filepath = None

        self._local_dir = None
        self._local_dir_bytes = None
        self._subreg_bytes = None

        self._db = None
        self._db_filepath = None
        self._db_map_size = initial_reg_size
        self._db_map_size_filepath = None
        self._readonly = None
        self._opened = False

        self._version = CURRENT_VERSION
        self._version_filepath = None

        self._cls_filepath = None

        self._startn_head = _START_N_HEAD_DEFAULT
        self._startn_tail_length = _START_N_TAIL_LENGTH_DEFAULT
        self._startn_tail_mod = 10 ** self._startn_tail_length
        self._length_length = _LENGTH_LENGTH_DEFAULT
        self._max_length = _MAX_LENGTH_DEFAULT

        self._ram_blks = {}

        self._created = False

    def __init_subclass__(cls, **kwargs):

        super().__init_subclass__(**kwargs)
        Register._constructors[cls.__name__] = cls

    #################################
    #     PROTEC INITIALIZATION     #

    _constructors = {}

    _instances = {}

    @staticmethod
    def _from_local_dir(local_dir):
        """Return a `Register` instance from a `local_dir` with the correct concrete subclass.

        This static method does not open the LMDB database at any point.

        :param local_dir: (type `pathlib.Path`) Absolute.
        :return: (type `Register`)
        """

        if not local_dir.is_absolute():
            raise ValueError(NOT_ABSOLUTE_ERROR_MESSAGE.format(str(local_dir)))

        if not local_dir.exists():
            raise FileNotFoundError(f"The `Register` database `{str(local_dir)}` could not be found.")

        check_reg_structure(local_dir)

        if Register._instance_exists(local_dir):
            # return the `Register` that has already been opened
            return Register._get_instance(local_dir)

        else:

            cls_name = read_txt_file(local_dir / CLS_FILEPATH)

            if cls_name == "Register":
                raise TypeError(
                    "`Register` is an abstract class, meaning that `Register` itself cannot be instantiated, " +
                    "only its concrete subclasses."
                )

            con = Register._constructors.get(cls_name, None)

            if con is None:
                raise TypeError(
                    f"`Register` is not aware of a subclass called `{cls_name}`. Please be sure that `{cls_name}` "
                    f"properly subclasses `Register` and that `{cls_name}` is in the namespace by importing it."
                )

            shorthand = read_txt_file(local_dir / SHORTHAND_FILEPATH)
            msg = read_txt_file(local_dir / MSG_FILEPATH)
            map_size = int(read_txt_file(local_dir / MAP_SIZE_FILEPATH))
            reg = con(local_dir.parent, shorthand, msg, map_size)
            reg._set_local_dir(local_dir)
            reg._version = read_txt_file(local_dir / VERSION_FILEPATH)
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
            raise RegisterError(_NOT_CREATED_ERROR_MESSAGE.format("__eq__"))

        elif type(self) != type(other):
            return False

        else:
            return self._local_dir == other._local_dir

    def __hash__(self):

        if not self._created:
            raise RegisterError(_NOT_CREATED_ERROR_MESSAGE.format("__hash__"))

        else:
            return hash(str(self._local_dir)) + hash(type(self))

    def __str__(self):

        if self._created:
            return f'{self._shorthand} ({self._local_dir}): "{self._msg}"'

        else:
            return f'{self._shorthand}: "{self._msg}"'

    def __repr__(self):
        return f'{self.__class__.__name__}("{str(self.saves_dir)}", "{self._shorthand}", "{self._msg}", {self._db_map_size})'

    def set_shorthand(self, shorthand):

        check_type(shorthand, "shorthand", str)

        if self._created:
            write_txt_file(shorthand, self._shorthand_filepath, True)

        self._shorthand = shorthand

    def set_msg(self, message, append = False):
        """Give this `Register` a detailed description.

        :param message: (type `str`)
        :param append: (type `bool`, default `False`) Set to `True` to append the new message to the old one.
        """

        check_type(message, "message", str)

        if append:
            new_msg = self._msg + message

        else:
            new_msg = message

        if self._created:
            write_txt_file(new_msg, self._msg_filepath, True)

        self._msg = new_msg

    def set_startn_info(self, head = None, tail_len = None):
        """Set the range of the `startn` parameters of disk `Block`s belonging to this `Register`.

        Reset to default `head` and `tail_len` by omitting the parameters.

        If the `startn` parameter is very large (of order more than trillions), then the `Register` database can
        become very bloated by storing many redundant digits for the `startn` parameter. Calling this method with
        appropriate `head` and `tail_len` parameters alleviates the bloat.

        The "head" and "tail" of a non-negative number x is defined by x = head * 10^L + tail, where L is the "length",
        or the number of digits, of "tail". (L must be at least 1, and 0 is considered to have 1 digit.)

        By calling `set_startn_info(head, tail_len)`, the user is asserting that the `startn` of every disk
        `Block` belong to this `Register` can be decomposed in the fashion startn = head * 10^tail_length + tail. The
        user is discouraged to call this method for large `tail_len` values (>12), as this is likely unnecessary and
        defeats the purpose of this method.

        :param head: (type `int`, optional) Non-negative. If omitted, resets this `Register` to the default `head`.
        :param tail_len: (type `int`) Positive. If omitted, resets this `Register` to the default `tail_len`.
        """

        # DEBUG : 1, 2

        self._check_open_raise("set_startn_info")

        self._check_readwrite_raise("set_startn_info")

        if head is not None and not is_int(head):
            raise TypeError("`head` must be of type `int`.")

        elif head is not None:
            head = int(head)

        else:
            head = _START_N_HEAD_DEFAULT

        if tail_len is not None and not is_int(tail_len):
            raise TypeError("`tail_len` must of of type `int`.")

        elif tail_len is not None:
            tail_len = int(tail_len)

        else:
            tail_len = _START_N_TAIL_LENGTH_DEFAULT

        if head < 0:
            raise ValueError("`head` must be non-negative.")

        if tail_len <= 0:
            raise ValueError("`tail_len` must be positive.")

        if head == self._startn_head and tail_len == self._startn_tail_length:
            return

        new_mod = 10 ** tail_len

        # check that every block startn has the correct head (don't change anything yet)
        with lmdb_prefix_iter(self._db, _BLK_KEY_PREFIX) as it:

            for key, _ in it:

                apri, startn, length = self._convert_disk_block_key(_BLK_KEY_PREFIX_LEN, key)

                if startn // new_mod != head:

                    raise ValueError(
                        "The following `startn` does not have the correct head:\n" +
                        f"`startn`   : {startn}\n" +
                        "That `startn` is associated with a `Block` whose `ApriInfo` and length is:\n" +
                        f"`ApriInfo` : {str(apri)}\n" +
                        f"length     : {length}\n"
                    )

        if _debug == 1:
            raise KeyboardInterrupt

        try:

            with self._db.begin(write = True) as rw_txn:

                with self._db.begin() as ro_txn:

                    with lmdb_prefix_iter(ro_txn, _BLK_KEY_PREFIX) as it:

                        rw_txn.put(_START_N_HEAD_KEY, bytify_num(head))
                        rw_txn.put(_START_N_TAIL_LENGTH_KEY, bytify_num(tail_len))

                        for key, val in it:

                            _, startn, _ = self._convert_disk_block_key(_BLK_KEY_PREFIX_LEN, key)
                            apri_id, _, length_bytes = self._split_disk_block_key(_BLK_KEY_PREFIX_LEN, key)
                            new_startn_bytes = bytify_num(startn % new_mod, tail_len)
                            new_key = Register._join_disk_block_data(
                                _BLK_KEY_PREFIX, apri_id, new_startn_bytes, length_bytes
                            )

                            if key != new_key:

                                rw_txn.put(new_key, val)
                                rw_txn.delete(key)

                if _debug == 2:
                    raise KeyboardInterrupt

        except lmdb.MapFullError as e:
            raise RegisterError(_MEMORY_FULL_ERROR_MESSAGE.format(self._db_map_size)) from e

        self._startn_head = head
        self._startn_tail_length = tail_len
        self._startn_tail_mod = 10 ** self._startn_tail_length

    @contextmanager
    def open(self, readonly = False):

        if not self._created and not readonly:

            # set local directory info and create levelDB database
            local_dir = random_unique_filename(self.saves_dir, length = 4, alphabet = LOCAL_DIR_CHARS)

            try:

                local_dir.mkdir()
                (local_dir / REG_FILENAME).mkdir()
                write_txt_file(self._shorthand, local_dir / SHORTHAND_FILEPATH)
                write_txt_file(self._msg, local_dir / MSG_FILEPATH)
                write_txt_file(self._version, local_dir / VERSION_FILEPATH)
                write_txt_file(str(type(self).__name__), local_dir / CLS_FILEPATH)
                write_txt_file(str(self._db_map_size), local_dir / MAP_SIZE_FILEPATH)
                (local_dir / DATABASE_FILEPATH).mkdir()
                self._set_local_dir(local_dir)
                self._db = open_lmdb(self._db_filepath, self._db_map_size, False)

                try:

                    with self._db.begin(write = True) as txn:
                        # set register info
                        txn.put(_START_N_HEAD_KEY, str(self._startn_head).encode("ASCII"))
                        txn.put(_START_N_TAIL_LENGTH_KEY, str(self._startn_tail_length).encode("ASCII"))
                        txn.put(_LENGTH_LENGTH_KEY, str(_LENGTH_LENGTH_DEFAULT).encode("ASCII"))
                        txn.put(_CURR_ID_KEY, b"0")

                except lmdb.MapFullError as e:
                    raise RegisterError(_MEMORY_FULL_ERROR_MESSAGE.format(self._db_map_size)) from e

                Register._add_instance(local_dir, self)
                yiel = self
                yiel._opened = True

            except BaseException as e:

                if local_dir.is_dir():
                    shutil.rmtree(local_dir)

                raise e

        elif self._created:
            yiel = self._open_created(readonly)

        else:
            raise ValueError(
                "You must `open` this `Register` at least once with `readonly = False` before you can open it in "
                "read-only mode."
            )

        try:
            yield yiel

        finally:
            yiel._close_created()

    @staticmethod
    @contextmanager
    def opens(*regs, **kwargs):

        if (len(kwargs) == 1 and 'readonlys' not in kwargs) or len(kwargs) > 1:
            raise KeyError("`opens` only takes one keyword-argument, `readonlys`.")

        if len(kwargs) == 1:
            readonlys = kwargs['readonlys']

        else:
            readonlys = None

        if readonlys is not None and not isinstance(readonlys, (list, tuple)):
            raise TypeError("`readonlys` must be of type `list` or `tuple`.")

        for reg in regs:

            if not isinstance(reg, Register):
                raise TypeError("Each element of `regs` must be of type `Register`.")

        if readonlys is not None:

            for readonly in readonlys:

                if not isinstance(readonly, bool):
                    raise TypeError("Each element of `readonlys` must be of type `bool`.")

        if readonlys is not None and len(regs) != len(readonlys):
            raise ValueError("`regs` and `readonlys` must have the same length.")

        if readonlys is None:
            readonlys = (False,) * len(regs)

        stack = ExitStack()
        yld = []

        with stack:

            for reg, readonly in zip(regs, readonlys):
                yld.append(stack.enter_context(reg.open(readonly = readonly)))

            yield tuple(yld)

    def increase_reg_size(self, num_bytes):
        """WARNING: DO NOT CALL THIS METHOD FROM MORE THAN ONE PYTHON PROCESS AT A TIME. You are safe if you call it
        from only one Python process. You are safe if you have multiple Python processes running and call it from only
        ONE of them. But do NOT call it from multiple processes at once. Doing so may result in catastrophic loss of
        data.

        :param num_bytes: (type `int`) Positive.
        """

        self._check_open_raise("increase_reg_size")

        if not is_int(num_bytes):
            raise TypeError("`num_bytes` must be of type `int`.")

        if num_bytes <= 0:
            raise ValueError("`num_bytes` must be positive.")

        if num_bytes <= self._db_map_size:
            raise ValueError("`num_bytes` must be larger than the current `Register` size.")

        self._db.set_mapsize(num_bytes)
        self._db_map_size = num_bytes
        write_txt_file(str(self._db_map_size), self._db_map_size_filepath, True)

    def reg_size(self):
        return self._db_map_size

    def ident(self):

        if not self._created:
            raise RegisterError(_NOT_CREATED_ERROR_MESSAGE.format("ident"))

        return str(self._local_dir)

    def shorthand(self):
        return self._shorthand

    #################################
    #    PROTEC REGISTER METHODS    #

    def _open_created(self, readonly):

        if Register._instance_exists(self._local_dir):
            ret = Register._get_instance(self._local_dir)

        else:
            ret = self

        if not ret._created:
            raise RegisterError(_NOT_CREATED_ERROR_MESSAGE.format("_open_created"))

        if ret._db is not None and ret._opened:
            raise RegisterAlreadyOpenError()

        ret._readonly = readonly
        ret._db = open_lmdb(ret._db_filepath, ret._db_map_size, readonly)

        with ret._db.begin() as txn:
            ret._length_length = int(txn.get(_LENGTH_LENGTH_KEY))

        ret._max_length = 10 ** ret._length_length - 1
        ret._opened = True
        return ret

    def _close_created(self):

        self._opened = False
        self._db.close()

    @contextmanager
    def _recursive_open(self, readonly):

        if not self._created:
            raise RegisterError(_NOT_CREATED_ERROR_MESSAGE.format("_recursive_open"))

        else:

            try:
                yiel = self._open_created(readonly)
                need_close = True

            except RegisterAlreadyOpenError:
                yiel = self
                need_close = False

            if not readonly and yiel._readonly:
                raise ValueError(
                    "Attempted to open a `Register` in read-write mode that is already open in read-only mode."
                )

            try:
                yield yiel

            finally:
                
                if need_close:
                    
                    yiel._close_created()
                    yiel._opened = False

    def _check_open_raise(self, method_name):

        if not self._opened:
            raise RegisterError(
                f"This `Register` database has not been opened. You must open this register via `with reg.open() as " +
                f"reg:` before calling the method `{method_name}`."
            )

    def _check_readwrite_raise(self, method_name):
        """Call `self._check_open_raise` before this method."""

        if self._readonly:
            raise RegisterError(
                f"This `Register` is `open`ed in read-only mode. In order to call the method `{method_name}`, you must "
                "open this `Register` in read-write mode via `with reg.open() as reg:`."
            )

    # def _check_memory_raise(self, keys, vals):
    #
    #     stat = self._db.stat()
    #
    #     current_size = stat.psize * (stat.leaf_pages + stat.branch_pages + stat.overflow_pages)
    #
    #     entry_size_bytes = sum(len(key) + len(value) for key, value in zip(keys, vals)) * BYTES_PER_CHAR
    #
    #     if current_size + entry_size_bytes >= Register._MEMORY_FULL_PROP * self._dbMapSize:
    #
    #         raise MemoryError(
    #             "The `Register` database is out of memory. Please allocate more memory using the method "
    #             "`Register.increase_reg_size`."
    #         )

    def _set_local_dir(self, local_dir):
        """`local_dir` and a corresponding register database must exist prior to calling this method.

        :param local_dir: (type `pathlib.Path`) Absolute.
        """

        if not local_dir.is_absolute():
            raise ValueError(NOT_ABSOLUTE_ERROR_MESSAGE.format(str(local_dir)))

        if local_dir.parent != self.saves_dir:
            raise ValueError(
                "The `local_dir` argument must be a sub-directory of `reg.savesDir`.\n" +
                f"`local_dir.parent`    : {str(local_dir.parent)}\n"
                f"`reg.savesDir` : {str(self.saves_dir)}"
            )

        check_reg_structure(local_dir)

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
        self._db_map_size_filepath = local_dir / MAP_SIZE_FILEPATH

    def _has_compatible_version(self):
        return self._version in COMPATIBLE_VERSIONS
    #
    # def _db_is_closed(self):
    #
    #     if not self._created:
    #         raise RegisterError(_NOT_CREATED_ERROR_MESSAGE.format("_db_is_closed"))
    #
    #     else:
    #         return lmdb_is_closed(self._db)

    #################################
    #      PUBLIC APRI METHODS      #

    def apris(self, diskonly = False, recursively = False):

        self._check_open_raise("apris")
        check_type(diskonly, "diskonly", bool)
        check_type(recursively, "recursively", bool)

        return list(self._apris_helper(diskonly, recursively, True))

    def change_apri(self, old_apri, new_apri, diskonly = False, recursively = False):
        """Replace an old `ApriInfo`, and all references to it, with a new `ApriInfo`.

        If ANY `Block`, `ApriInfo`, or `AposInfo` references `old_apri`, its entries in this `Register` will be
        updated to reflect the replacement of `old_apri` with `new_apri`. (See example below.) After the replacement
        `old_apri` -> `new_apri` is made, the set of `ApriInfo` that changed under that replacement must be disjoint
        from the set of `ApriInfo` that did not change. Otherwise, a `ValueError` is raised.

        For example, say we intend to replace

        `old_apri = ApriInfo(descr = "periodic points")`

        with

        `new_apri = ApriInfo(descr = "periodic points", ref = "Newton et al. 2005")`.

        In an example `Register`, there are two `Block`s, one with `old_apri` and the other with

        `some_other_apri = ApriInfo(descr = "period length", respective = old_apri)`.

        After a call to `change_apri(old_apri, new_apri)`, the first `Block` will have `new_apri` and the second
        will have

        `ApriInfo(descr = "period length", respective = new_apri)`.

        :param old_apri: (type `ApriInfo`)
        :param new_apri: (type `ApriInfo`)
        :param recursively: (type `bool`)
        :raise ValueError: See above.
        """

        # DEBUG : 1, 2, 3

        self._check_open_raise("change_apri")

        self._check_readwrite_raise("change_apri")

        self._check_known_apri(old_apri)

        if old_apri == new_apri:
            return

        # if not diskonly:
        #
        #     for apri in self._ram_blks.keys():
        #         apri.change_info(old_apri, new_apri)

        try:

            with self._db.begin(write = True) as txn:

                if _debug == 1:
                    raise KeyboardInterrupt

                try:
                    self._check_known_apri(new_apri, txn)

                except DataNotFoundError:
                    has_new_apri_already = False

                else:
                    has_new_apri_already = True

                if has_new_apri_already:
                    warnings.warn(f"This `Register` already has a reference to {str(new_apri)}.")

                # delete old_apri and reserve its ID
                old_id = self._get_id_by_apri(old_apri, None, False, txn, None)
                old_id_apri_key = _ID_APRI_KEY_PREFIX + old_id
                old_apri_json = txn.get(old_id_apri_key)
                old_apri_id_key = _APRI_ID_KEY_PREFIX + old_apri_json
                txn.delete(old_id_apri_key)
                txn.delete(old_apri_id_key)
                reserved = [old_id]

                if _debug == 2:
                    raise KeyboardInterrupt

                # make inner id's if they're missing
                for key, info in new_apri.iter_inner_info(mode = "dfs"):

                    if key is not None and isinstance(info, ApriInfo):
                        self._get_id_by_apri(info, None, True, txn, reserved)

                # check for duplicate keys and give old_id new new_apri
                new_apri_json = relational_encode_info(self, new_apri, txn)
                new_apri_id_key = _APRI_ID_KEY_PREFIX + new_apri_json

                if lmdb_has_key(txn, new_apri_id_key):
                    raise ValueError("Duplicate `ApriInfo`.")

                Register._add_apri(txn, old_id, new_apri, new_apri_json)

                if _debug == 3:
                    raise KeyboardInterrupt

        except lmdb.MapFullError as e:
            raise RegisterError(_MEMORY_FULL_ERROR_MESSAGE.format(self._db_map_size)) from e

        if recursively:

            for subreg in self._iter_subregs():

                with subreg._recursive_open(False) as subreg:
                    subreg.change_apri(old_apri, new_apri, True)

    def rmv_apri(self, apri, force = False, missing_ok = False):
        """Remove an `ApriInfo` that is not associated with any other `ApriInfo`, `Block`, nor `AposInfo`.

        :param apri: (type `ApriInfo`)
        :raise ValueError: If there are any `ApriInfo`, `Block`, or `AposInfo` associated with `info`.
        """

        # DEBUG : 1, 2, 3, 4, 5

        self._check_open_raise("rmv_apri")
        self._check_readwrite_raise("rmv_apri")
        check_type(apri, "apri", ApriInfo)
        check_type(force, "force", bool)
        check_type(missing_ok, "missing_ok", bool)

        if not missing_ok:
            self._check_known_apri(apri)

        txn = None
        ram_blk_del_success = False
        reinsert = None

        try:

            if apri in self._ram_blks.keys():

                reinsert = self._ram_blks[apri]
                del self._ram_blks[apri]
                ram_blk_del_success = True

            with ReversibleTransaction(self._db).begin(write = True) as txn:
                blkDatas = self._rmv_apri_txn(apri, force, txn)

            for apri_, startn, length in blkDatas:
                self.rmv_disk_blk(apri_, startn, length, False, False)

        except BaseException as e:

            if ram_blk_del_success:
                self._ram_blks[apri] = reinsert

            if txn is not None:

                with self._db.begin(write = True) as txn_:
                    txn.reverse(txn_)

            if isinstance(e, lmdb.MapFullError):
                raise RegisterError(_MEMORY_FULL_ERROR_MESSAGE.format(self._db_map_size)) from e

            else:
                raise e

    def __contains__(self, apri):

        self._check_open_raise("__contains__")

        if any(blk.apri() == apri for blk in self._ram_blks):
            return True

        else:

            with self._db.begin() as txn:

                try:
                    apri_json = relational_encode_info(self, apri, txn)

                except DataNotFoundError:
                    return False

                key = _APRI_ID_KEY_PREFIX + apri_json
                return lmdb_has_key(txn, key)

    def __iter__(self):
        return iter(self.apris())

    #################################
    #      PROTEC APRI METHODS      #

    def _apris_helper(self, diskonly, recursively, root_call):

        ret = []

        if not diskonly:
            ret.extend(self._ram_blks.keys())

        with self._db.begin() as txn:

            with lmdb_prefix_iter(txn, _ID_APRI_KEY_PREFIX) as it:

                for _, val in it:
                    ret.append(relational_decode_info(self, ApriInfo, val, txn))

        if recursively:

            for subreg in self._iter_subregs():

                with subreg._recursive_open(True) as subreg:
                    ret.extend(subreg._apris_helper(diskonly, recursively, False))

        if root_call:
            yield from sorted(set(ret))

        else:
            yield from ret

    def _check_known_apri(self, apri, txn = None):

        commit = txn is None

        if commit:
            txn = self._db.begin()

        try:

            try:
                self._get_id_by_apri(apri, None, False, txn, None)

            except DataNotFoundError:
                pass

            else:
                return

        finally:

            if commit:
                txn.commit()

        if apri not in self._ram_blks.keys():
            raise DataNotFoundError(_NO_APRI_ERROR_MESSAGE.format(str(apri)))

    def _get_apri_json_by_id(self, id_, txn = None):
        """Get JSON bytestring representing an `ApriInfo` instance.

        :param id_: (type `bytes`)
        :param txn: (type `lmbd.Transaction`, default `None`) The transaction to query. If `None`, then use open a new
        transaction and commit it after this method resolves.
        :return: (type `bytes`)
        """

        commit = txn is None

        if commit:
            txn = self._db.begin()

        try:
            return txn.get(_ID_APRI_KEY_PREFIX + id_)

        finally:

            if commit:
                txn.commit()

    def _get_id_by_apri(self, apri, apri_json, missing_ok, txn = None, reserved = None):
        """Get an `ApriInfo` ID for this database. If `missing_ok is True`, then create an ID if the passed `apri` or
        `apri_json` is unknown to this `Register`.

        One of `apri` and `apri_json` can be `None`, but not both. If both are not `None`, then `apri` is used.

        `self._db` must be opened by the caller.

        :param apri: (type `ApriInfo`)
        :param apri_json: (type `bytes`)
        :param missing_ok: (type `bool`) Create an ID if the passed `apri` or `apri_json` is unknown to this `Register`.
        :param txn: (type `lmbd.Transaction`, default `None`) The transaction to query. If `None`, then open a new
        transaction and commit it after this method returns.
        :param reserved: (type `list`, default `None`) Apri ID's that cannot be returned by this method.
        :raises DataNotFoundError: If `apri` or `apri_json` is not known to this `Register` and `missing_ok
        is False`.
        :return: (type `bytes`)
        """

        if apri is not None:
            apri_json =  relational_encode_info(self, apri, txn)

        elif apri_json is None:
            raise ValueError

        commit = txn is None
        key = _APRI_ID_KEY_PREFIX + apri_json

        if commit and missing_ok:
            txn = self._db.begin(write = True)

        elif commit:
            txn = self._db.begin()

        try:

            id_ = txn.get(key, default = None)

            if id_ is not None:
                return id_

            elif missing_ok:

                next_id = Register._get_new_apri_id(txn, reserved if reserved is not None else [])
                Register._add_apri(txn, next_id, apri, apri_json)
                return next_id

            else:

                if apri is None:
                    apri = relational_decode_info(self, ApriInfo, apri_json, txn)

                raise DataNotFoundError(_NO_APRI_ERROR_MESSAGE.format(str(apri)))

        finally:

            if commit:

                try:
                    txn.commit()

                except lmdb.MapFullError as e:
                    raise RegisterError(_MEMORY_FULL_ERROR_MESSAGE.format(self._db_map_size)) from e

    @staticmethod
    def _add_apri(txn, id_, apri, apri_json):

        apri_id_key = _APRI_ID_KEY_PREFIX + apri_json
        id_apri_key = _ID_APRI_KEY_PREFIX + id_
        txn.put(apri_id_key, id_)
        txn.put(id_apri_key, apri_json)

        if 8 + 6 + len(apri_id_key) > 4096:
            warnings.warn(f"Long `ApriInfo` result in disk memory inefficiency. Long `ApriInfo`: {str(apri)}.")

    @staticmethod
    def _get_new_apri_id(txn, reserved):

        for next_id_num in range(int(txn.get(_CURR_ID_KEY)), _MAX_NUM_APRI):

            next_id = bytify_num(next_id_num, _MAX_NUM_APRI_LENGTH)

            if next_id not in reserved:
                break

        else:
            raise RegisterError(f"Too many apris added to this `Register`, the limit is {_MAX_NUM_APRI}.")

        txn.put(_CURR_ID_KEY, bytify_num(next_id_num + 1, _MAX_NUM_APRI_LENGTH))
        return next_id

    def _rmv_apri_txn(self, apri, force, txn):

        apris = []
        aposs = []
        blk_datas = []
        self._rmv_apri_txn_helper(txn, apri, apris, aposs, blk_datas, force)

        if force:

            for data in blk_datas:
                self._rmv_disk_blk_txn(data[0], data[1], data[2], txn)

            for apri in aposs:
                self._rmv_apos_info_txn(apri, txn)

            for apri in apris:
                self._rmv_apri_txn(apri, False, txn)

        apri_json = relational_encode_info(self, apri, txn)
        apri_id = self._get_id_by_apri(apri, apri_json, False, txn, None)
        txn.delete(_ID_APRI_KEY_PREFIX + apri_id)
        txn.delete(_APRI_ID_KEY_PREFIX + apri_json)

        return blk_datas

    def _rmv_apri_txn_helper(self, txn, apri, apris, aposs, blk_datas, force):

        apris.append(apri)

        if _debug == 1:
            raise KeyboardInterrupt

        if self._num_disk_blks_txn(apri, txn) != 0:

            if not force:

                raise ValueError(
                    f"There are disk `Block`s saved with `{str(apri)}`. Please remove them first and call "
                    "`rmv_apri` again. Or remove them automatically by calling "
                    "`reg.rmv_apri(info, force = True)`."
                )

            else:

                for key, val in self._iter_disk_blk_pairs(_BLK_KEY_PREFIX, apri, None, txn):

                    blk_filename = val.decode("ASCII")

                    if not is_deletable(self._local_dir / blk_filename):
                        raise OSError(f"Cannot delete `Block` file `{blk_filename}`.")

                    blk_datas.append(self._convert_disk_block_key(_BLK_KEY_PREFIX_LEN, key))

                for key, val in self._iter_disk_blk_pairs(_COMPRESSED_KEY_PREFIX, apri, None, txn):

                    compr_filename = val.decode("ASCII")

                    if val != _IS_NOT_COMPRESSED_VAL and not is_deletable(self._local_dir / compr_filename):
                        raise OSError(f"Cannot delete compressed `Block` file `{compr_filename}`.")

        if _debug == 2:
            raise KeyboardInterrupt

        try:
            self.apos(apri)

        except DataNotFoundError:
            pass

        else:

            if not force:

                raise ValueError(
                    f"There is an `AposInfo` associated with `{str(apri)}`. Please remove it first and call "
                    "`rmv_apri` again. Or remove automatically by calling `reg.rmv_apri(info, force = True)`."
                )

            else:
                aposs.append(apri)

        if _debug == 3:
            raise KeyboardInterrupt

        with lmdb_prefix_iter(txn, _ID_APRI_KEY_PREFIX) as it:

            for _, apri_json_ in it:

                apri_ = relational_decode_info(self, ApriInfo, apri_json_, txn)

                if apri in apri_ and apri != apri_:

                    if not force:

                        raise ValueError(
                            f"{str(apri_)} is associated with {str(apri)}. Please remove the former first before "
                            f"removing the latter. Or remove automatically by calling `reg.rmv_apri(info, "
                            f"force = True)`."
                        )

                    else:
                        self._rmv_apri_txn_helper(txn, apri_, apris, aposs, blk_datas, True)

            if _debug == 4:
                raise KeyboardInterrupt

    #################################
    #      PUBLIC APOS METHODS      #

    def set_apos(self, apri, apos):
        """Set some `AposInfo` for corresponding `ApriInfo`.

        WARNING: This method will OVERWRITE any previous saved `AposInfo`. If you do not want to lose any previously
        saved data, then you should do something like the following:

            apos = reg.apos(info)
            apos.period_length = 5
            reg.set_apos(apos)

        :param apri: (type `ApriInfo`)
        :param apos: (type `AposInfo`)
        """

        # DEBUG : 1, 2

        self._check_open_raise("set_apos")
        self._check_readwrite_raise("set_apos")
        check_type(apri, "apri", ApriInfo)
        check_type(apos, "apos", AposInfo)

        if _debug == 1:
            raise KeyboardInterrupt

        try:

            with self._db.begin(write = True) as txn:

                self._set_apos_info_txn(apri, apos, txn)

                if _debug == 2:
                    raise KeyboardInterrupt

        except lmdb.MapFullError as e:
            raise RegisterError(_MEMORY_FULL_ERROR_MESSAGE.format(self._db_map_size)) from e

    def apos(self, apri):
        """Get some `AposInfo` associated with a given `ApriInfo`.

        :param apri: (type `ApriInfo`)
        :raises DataNotFoundError: If no `AposInfo` has been associated to `info`.
        :return: (type `AposInfo`)
        """

        self._check_open_raise("apos")
        check_type(apri, "apri", ApriInfo)

        with self._db.begin() as txn:

            key = self._get_apos_key(apri, None, False, txn)
            apos_json = txn.get(key, default=None)

            if apos_json is not None:
                return relational_decode_info(self, AposInfo, apos_json, txn)

            else:
                raise DataNotFoundError(f"No `AposInfo` associated with `{str(apri)}`.")

    def rmv_apos(self, apri, missing_ok = False):

        # DEBUG : 1, 2

        self._check_open_raise("rmv_apos")
        self._check_readwrite_raise("rmv_apos")
        check_type(apri, "apri", ApriInfo)
        check_type(missing_ok, "missing_ok", bool)

        if _debug == 1:
            raise KeyboardInterrupt

        try:

            with self._db.begin(write = True) as txn:

                self._rmv_apos_info_txn(apri, txn)

                if _debug == 2:
                    raise KeyboardInterrupt

        except lmdb.MapFullError as e:
            raise RegisterError(_MEMORY_FULL_ERROR_MESSAGE.format(self._db_map_size)) from e

        except DataNotFoundError:

            if not missing_ok:
                raise

    #################################
    #      PROTEC APOS METHODS      #

    def _set_apos_info_txn(self, apri, apos, txn):

        key = self._get_apos_key(apri, None, True, txn)
        apos_json = relational_encode_info(self, apos, txn)
        txn.put(key, apos_json)

        if 6 + 8 + _APOS_KEY_PREFIX_LEN +  len(apos_json) > 4096:
            warnings.warn(f"Long `AposInfo` result in disk memory inefficiency. Long `AposInfo`: {str(apri)}.")

    def _rmv_apos_info_txn(self, apri, txn):

        key = self._get_apos_key(apri, None, False, txn)

        if lmdb_has_key(txn, key):
            txn.delete(key)

        else:
            raise DataNotFoundError(f"No `AposInfo` associated with `{str(apri)}`.")

    def _get_apos_key(self, apri, apri_json, missing_ok, txn = None):
        """Get a key for an `AposInfo` entry.

        One of `apri` and `apri_json` can be `None`, but not both. If both are not `None`, then `apri` is used. If
        `missing_ok is True`, then create a new `ApriInfo` ID if one does not already exist for `apri`.

        :param apri: (type `ApriInfo`)
        :param apri_json: (type `bytes`)
        :param missing_ok: (type `bool`)
        :param txn: (type `lmbd.Transaction`, default `None`) The transaction to query. If `None`, then use open a new
        transaction and commit it after this method resolves.
        :raises DataNotFoundError: If `missing_ok is False` and `apri` is not known to this `Register`.
        :return: (type `bytes`)
        """

        if apri is None and apri_json is None:
            raise ValueError

        apri_id = self._get_id_by_apri(apri, apri_json, missing_ok, txn, None)

        return _APOS_KEY_PREFIX + _KEY_SEP + apri_id

    #################################
    #  PUBLIC SUB-REGISTER METHODS  #

    def add_subreg(self, subreg, exists_ok = False):

        # DEBUG : 1, 2

        self._check_open_raise("add_subreg")
        self._check_readwrite_raise("add_subreg")
        check_type(subreg, "subreg", Register)
        check_type(exists_ok, "exists_ok", bool)

        if not subreg._created:
            raise RegisterError(_NOT_CREATED_ERROR_MESSAGE.format("add_subreg"))

        if _debug == 1:
            raise KeyboardInterrupt

        try:

            with self._db.begin(write = True) as txn:
                self._add_subreg_txn(subreg, txn)

        except lmdb.MapFullError as e:
            raise RegisterError(_MEMORY_FULL_ERROR_MESSAGE.format(self._db_map_size)) from e

        except RegisterError as e:

            if str(e) == _REG_ALREADY_ADDED_ERROR_MESSAGE:

                if not exists_ok:
                    raise

            else:
                raise

    def rmv_subreg(self, subreg, missing_ok = False):
        """
        :param subreg: (type `Register`)
        """

        # DEBUG : 1, 2

        self._check_open_raise("rmv_subreg")
        self._check_readwrite_raise("rmv_subreg")
        check_type(subreg, "Register", Register)
        check_type(missing_ok, "missing_ok", bool)

        if _debug == 1:
            raise KeyboardInterrupt

        try:

            with self._db.begin(write = True) as txn:

                self._rmv_subreg_txn(subreg, txn)

                if _debug == 2:
                    raise KeyboardInterrupt

        except lmdb.MapFullError as e:
            raise RegisterError(_MEMORY_FULL_ERROR_MESSAGE.format(self._db_map_size)) from e

        except DataNotFoundError:

            if not missing_ok:
                raise

    def subregs(self):
        return list(self._iter_subregs())

    #################################
    #  PROTEC SUB-REGISTER METHODS  #

    def _add_subreg_txn(self, subreg, txn):

        key = subreg._get_subreg_key()

        if not lmdb_has_key(txn, key):

            if subreg._check_no_cycles_from(self):

                txn.put(key, _SUB_VAL)

                if _debug == 2:
                    raise KeyboardInterrupt

            else:

                raise RegisterError(
                    "Attempting to add this register as a sub-register will created a directed cycle in the "
                    "subregister relation. "
                    f'Intended super-register description: "{str(self)}". '
                    f'Intended sub-register description: "{str(subreg)}".'
                )

        else:
            raise RegisterError(_REG_ALREADY_ADDED_ERROR_MESSAGE)

    def _rmv_subreg_txn(self, subreg, txn):

        key = subreg._get_subreg_key()

        if lmdb_has_key(txn, key):
            txn.delete(key)

        else:
            raise RegisterError(f"No subregister found with the following message : {str(subreg)}")

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
            raise RegisterError(_NOT_CREATED_ERROR_MESSAGE.format("_check_no_cycles_from"))

        if self is original:
            return False

        if touched is None:
            touched = set()

        with self._recursive_open(True) as reg:

            if any(
                original is subreg
                for subreg in reg._iter_subregs()
            ):
                return False

            for subreg in reg._iter_subregs():

                if subreg not in touched:

                    touched.add(subreg)
                    if not subreg._check_no_cycles_from(original, touched):
                        return False


            else:
                return True

    def _iter_subregs(self):

        with lmdb_prefix_iter(self._db, _SUB_KEY_PREFIX) as it:

            for key, _ in it:

                local_dir = Path(key[_SUB_KEY_PREFIX_LEN : ].decode("ASCII"))
                subreg = Register._from_local_dir(local_dir)
                yield subreg

    def _get_subreg_key(self):
        return _SUB_KEY_PREFIX + self._local_dir_bytes

    #################################
    #    PUBLIC DISK BLK METHODS    #

    @classmethod
    @abstractmethod
    def dump_disk_data(cls, data, filename, **kwargs):
        """Dump data to the disk.

        This method should not change any properties of any `Register`, which is why it is a class-method and
        not an instance-method. It merely takes `data`, processes it, and dumps it to disk.

        Most use-cases prefer the instance-method `add_disk_blk`.

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

        Most use-cases prefer the method `blk`.

        :param filename: (type `pathlib.Path`) Where to load the block from. You may need to edit this
        filename if necessary, such as by adding a suffix, but you must return the edited filename.
        :raises DataNotFoundError: If the data could not be loaded because it doesn't exist.
        :return: (any type) The data loaded from the disk.
        :return: (pathlib.Path) The exact path of the data saved to the disk.
        """

    @classmethod
    def clean_disk_data(cls, filename, **kwargs):
        """Remove raw data from the disk.

        This method should not change any properties of any `Register`, which is why it is a classmethod and
        not an instancemethod. It merely removes the raw data.

        Most use-cases prefer the method `rmv_disk_blk`.

        :param filename: (type `pathlib.Path`) Where to remove the raw data.
        :raises DataNotFoundError: If the data could not be cleaned because it doesn't exist.
        """

        if not filename.is_absolute():
            raise ValueError(NOT_ABSOLUTE_ERROR_MESSAGE.format(filename))

        filename.unlink()

    @classmethod
    @abstractmethod
    def with_suffix(cls, filename):
        """Adds a suffix to a filename and returns it.

        :param filename: (type `pathlib.Path`)
        :return: (type `pathlib.Path`)
        """

    def add_disk_blk(self, blk, exists_ok = False, dups_ok = True, ret_metadata = False, **kwargs):
        """Save a `Block` to disk and link it with this `Register`.

        :param blk: (type `Block`)
        :param ret_metadata: (type `bool`, default `False`) Whether to return a `File_Metadata` object, which
        contains file creation date/time and size of dumped data to the disk.
        :raises RegisterError: If a duplicate `Block` already exists in this `Register`.
        """

        #DEBUG : 1, 2, 3, 4, 5

        self._check_open_raise("add_disk_blk")
        self._check_readwrite_raise("add_disk_blk")
        check_type(blk, "blk", Block)
        check_type(exists_ok, "exists_ok", bool)
        check_type(dups_ok, "dups_ok", bool)
        check_type(ret_metadata, "ret_metadata", bool)
        startn_head = blk.startn() // self._startn_tail_mod

        if startn_head != self._startn_head :

            raise IndexError(
                "The `startn_` for the passed `Block` does not have the correct head:\n"
                f"`tail_len`      : {self._startn_tail_length}\n"
                f"expected `head` : {self._startn_head}\n"
                f"`startn`        : {blk.startn()}\n"
                f"`startn` head   : {startn_head}\n"
                "Please see the method `set_startn_info` to troubleshoot this error."
            )

        if len(blk) > self._max_length:
            raise ValueError

        if _debug == 1:
            raise KeyboardInterrupt

        txn = None
        filename = None

        if not dups_ok:

            int_ = (blk.startn(), len(blk))

            for t in self.intervals(blk.apri(), False, True, False):

                if intervals_overlap(t, int_):
                    raise RegisterError(
                        "Attempted to add a `Block` with duplicate indices. Set `dups_ok` to `True` to suppress."
                    )

        if _debug == 2:
            raise KeyboardInterrupt

        try:

            with ReversibleTransaction(self._db).begin(write = True) as txn:

                filename = self._add_disk_blk_txn(blk, txn)

                if _debug == 3:
                    raise KeyboardInterrupt

            if _debug == 4:
                raise KeyboardInterrupt

            type(self).dump_disk_data(blk.segment(), filename, **kwargs)

            if _debug == 5:
                raise KeyboardInterrupt

        except BaseException as e:

            if not isinstance(e, RegisterError) or not exists_ok or not "exist" in str(e):

                try:

                    if filename is not None:

                        try:
                            filename.unlink()

                        except FileNotFoundError:
                            pass

                    if txn is not None:

                        with self._db.begin(write = True) as txn_:
                            txn.reverse(txn_)

                except:
                    raise RegisterRecoveryError("Could not successfully recover from a failed disk `Block` add!") from e

                else:

                    if isinstance(e, lmdb.MapFullError):
                        raise RegisterError(_MEMORY_FULL_ERROR_MESSAGE.format(self._db_map_size)) from e

                    else:
                        raise e

        if ret_metadata:
            return self.blk_metadata(blk.apri(), blk.startn(), len(blk), False)

        else:
            return None

    def append_disk_blk(self, blk, ret_metadata = False, **kwargs):
        """Add a `Block` to disk and link it with this `Register`.

        If no disk `Block`s with the same `ApriInfo` as `blk` have previously been added to disk, then `startn` is set
        to 0.. If not, then `startn` will be set to one more than the largest index among all disk `Block`s with the
        same `ApriInfo` as `blk`.

        :param blk: (type `Block`)
        :param ret_metadata: (type `bool`, default `False`) Whether to return a `File_Metadata` object, which
        contains file creation date/time and size of dumped data to the disk.
        :raises RegisterError: If a duplicate `Block` already exists in this `Register`.
        """

        check_type(blk, "blk", Block)
        check_type(ret_metadata, "ret_metadata", bool)

        if self.num_blks(blk.apri(), diskonly = True) == 0:
            return self.add_disk_blk(blk, ret_metadata, **kwargs)

        else:

            blk.set_startn(self.maxn(blk.apri()) + 1)
            return self.add_disk_blk(blk, ret_metadata, **kwargs)

    def rmv_disk_blk(self, apri, startn = None, length = None, missing_ok = False, recursively = False, **kwargs):
        """Delete a disk `Block` and unlink it with this `Register`.

        :param apri: (type `ApriInfo`)
        :param startn: (type `int`) Non-negative.
        :param length: (type `int`) Non-negative.
        :param recursively: (type `bool`)
        """

        # DEBUG : 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17

        self._check_open_raise("rmv_disk_blk")
        self._check_readwrite_raise("rmv_disk_blk")
        check_type(apri, "apri", ApriInfo)
        startn = check_return_int_None_default(startn, "startn", None)
        length = check_return_int_None_default(length, "length", None)
        check_type(missing_ok, "missing_ok", bool)
        check_type(recursively, "recursively", bool)

        if startn is not None and startn < 0:
            raise ValueError("`startn` must be non-negative.")

        if length is not None and length < 0:
            raise ValueError("`length` must be non-negative.")

        try:
            startn_, length_ = self._resolve_startn_length(apri, startn, length, True)

        except DataNotFoundError:
            pass

        else:

            if _debug == 1:
                raise KeyboardInterrupt

            txn = None
            blkFilename = None
            comprFilename = None

            if _debug == 2:
                raise KeyboardInterrupt

            try:

                if _debug == 3:
                    raise KeyboardInterrupt

                with ReversibleTransaction(self._db).begin(write = True) as txn:

                    if _debug == 4:
                        raise KeyboardInterrupt

                    blkFilename, comprFilename = self._rmv_disk_blk_txn(apri, startn_, length_, txn)

                    if _debug == 5:
                        raise KeyboardInterrupt

                if _debug == 6:
                    raise KeyboardInterrupt

                if not is_deletable(blkFilename):
                    raise OSError(f"Cannot delete `Block` file `{str(blkFilename)}`.")

                if _debug == 7:
                    raise KeyboardInterrupt

                if comprFilename is not None and not is_deletable(comprFilename):
                    raise OSError(f"Cannot delete compressed `Block` file `{str(comprFilename)}`.")

                if _debug == 8:
                    raise KeyboardInterrupt

                if comprFilename is not None:

                    blkFilename.unlink()

                    if _debug == 9:
                        raise KeyboardInterrupt

                    comprFilename.unlink()

                else:
                    type(self).clean_disk_data(blkFilename, **kwargs)

                return

            except BaseException as e:

                FAIL_NO_RECOVER_ERROR_MESSAGE = "Could not successfully recover from a failed disk `Block` remove!"

                try:

                    if comprFilename is not None:

                        if blkFilename is not None and comprFilename.exists():

                            try:
                                blkFilename.touch()

                            except FileExistsError:
                                pass

                        else:
                            raise RegisterRecoveryError(FAIL_NO_RECOVER_ERROR_MESSAGE) from e

                    elif blkFilename is not None and not blkFilename.exists():
                        raise RegisterRecoveryError(FAIL_NO_RECOVER_ERROR_MESSAGE) from e

                    if txn is not None:

                        with self._db.begin(write = True) as txn_:
                            txn.reverse(txn_)

                except RegisterRecoveryError:
                    raise

                except:
                    raise RegisterRecoveryError(FAIL_NO_RECOVER_ERROR_MESSAGE) from e

                else:

                    if isinstance(e, lmdb.MapFullError):
                        raise RegisterError(_MEMORY_FULL_ERROR_MESSAGE.format(self._db_map_size)) from e

                    else:
                        raise e

        if recursively:

            for subreg in self._iter_subregs():

                with subreg._recursive_open(False) as subreg:

                    try:
                        subreg.rmv_disk_blk(apri, startn, length, False, True, **kwargs)
                        return

                    except DataNotFoundError:
                        pass

        if not missing_ok:
            raise DataNotFoundError(_blk_not_found_err_msg(True, apri, None, startn, length))

    def blk_metadata(self, apri, startn = None, length = None, recursively = False):

        self._check_open_raise("blk_metadata")
        check_type(apri, "apri", ApriInfo)
        startn = check_return_int_None_default(startn, "startn", None)
        length = check_return_int_None_default(length, "length", None)
        check_type(recursively, "recursively", bool)

        if startn is not None and startn < 0:
            raise ValueError("`startn` must be non-negative.")

        if length is not None and length < 0:
            raise ValueError("`length` must be non-negative.")

        try:
            startn_, length_ = self._resolve_startn_length(apri, startn, length, True)

        except DataNotFoundError:
            pass

        else:

            try:
                blk_key, compressed_key = self._check_blk_compressed_keys_raise(
                    None, None, apri, None, startn_, length_
                )

            except DataNotFoundError:
                pass

            else:
                blk_filename, compressed_filename = self._check_blk_compressed_files_raise(
                    blk_key, compressed_key, apri, startn_, length_
                )

                if compressed_filename is not None:
                    return FileMetadata.from_path(compressed_filename)

                else:
                    return FileMetadata.from_path(blk_filename)

        if recursively:

            for subreg in self._iter_subregs():

                with subreg._recursive_open(True) as subreg:

                    try:
                        return subreg.blk_metadata(apri, startn, length, True)

                    except DataNotFoundError:
                        pass

        raise DataNotFoundError(
            _blk_not_found_err_msg(True, apri, None, startn, length)
        )

    def compress(self, apri, startn = None, length = None, compression_level = 6, ret_metadata = False):
        """Compress a `Block`.

        :param apri: (type `ApriInfo`)
        :param startn: (type `int`) Non-negative.
        :param length: (type `int`) Non-negative.
        :param compression_level: (type `int`, default 6) Between 0 and 9, inclusive. 0 is for the fastest compression,
        but lowest compression ratio; 9 is slowest, but highest ratio. See
        https://docs.python.org/3/library/zlib.html#zlib.compressobj for more information.
        :param ret_metadata: (type `bool`, default `False`) Whether to return a `File_Metadata` object that
        describes the compressed file.
        :raises CompressionError: If the `Block` is already compressed.
        :return: (type `File_Metadata`) If `ret_metadata is True`.
        """

        # DEBUG : 1, 2, 3, 4

        _FAIL_NO_RECOVER_ERROR_MESSAGE = "Could not recover successfully from a failed disk `Block` compress!"
        self._check_open_raise("compress")
        self._check_readwrite_raise("compress")
        check_type(apri, "apri", ApriInfo)
        startn = check_return_int_None_default(startn, "startn", None)
        length = check_return_int_None_default(length, "length", None)
        compression_level = check_return_int(compression_level, "compression_level")
        check_type(ret_metadata, "ret_metadata", bool)

        if startn is not None and startn < 0:
            raise ValueError("`startn` must be non-negative.")

        if length is not None and length < 0:
            raise ValueError("`length` must be non-negative.")

        if not (0 <= compression_level <= 9):
            raise ValueError("`compression_level` must be between 0 and 9, inclusive.")

        startn_, length_ = self._resolve_startn_length(apri, startn, length, True)

        compressed_key = self._get_disk_blk_key(
            _COMPRESSED_KEY_PREFIX, apri, None, startn_, length_, False
        )

        blk_key, compressed_key = self._check_blk_compressed_keys_raise(
            None, compressed_key, apri, None, startn_, length_
        )

        with self._db.begin() as txn:
            compressed_val = txn.get(compressed_key)

        if compressed_val != _IS_NOT_COMPRESSED_VAL:

            raise CompressionError(
                "The disk `Block` with the following data has already been compressed: " +
                f"{str(apri)}, startn = {startn_}, length = {length_}"
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
                compression_level

            ) as compressed_fh:

                compressed_fh.write(blk_filename, blk_filename.name)

                if _debug == 3:
                    raise KeyboardInterrupt

            if _debug == 4:
                raise KeyboardInterrupt

            type(self).clean_disk_data(blk_filename)
            cleaned = True
            blk_filename.touch()

        except BaseException as e:

            try:

                with self._db.begin(write = True) as txn:
                    txn.put(compressed_key, _IS_NOT_COMPRESSED_VAL)

                if cleaned or not blk_filename.exists():
                    raise RegisterRecoveryError(_FAIL_NO_RECOVER_ERROR_MESSAGE)

                else:

                    try:
                        compressed_filename.unlink()

                    except FileNotFoundError:
                        pass

            except RegisterRecoveryError as ee:
                raise ee

            except BaseException:
                raise RegisterRecoveryError(_FAIL_NO_RECOVER_ERROR_MESSAGE)

            else:

                if isinstance(e, lmdb.MapFullError):
                    raise RegisterError(_MEMORY_FULL_ERROR_MESSAGE.format(self._db_map_size)) from e

                else:
                    raise e

        if ret_metadata:
            return FileMetadata.from_path(compressed_filename)

        else:
            return None

    def decompress(self, apri, startn = None, length = None, ret_metadata = False):
        """Decompress a `Block`.

        :param apri: (type `ApriInfo`)
        :param startn: (type `int`) Non-negative.
        :param length: (type `int`) Non-negative.
        :param ret_metadata: (type `bool`, default `False`) Whether to return a `File_Metadata` object that
        describes the decompressed file.
        :raise DecompressionError: If the `Block` is not compressed.
        :return: (type `list`) If `ret_metadata is True`.
        """

        # DEBUG : 1, 2, 3, 4

        _FAIL_NO_RECOVER_ERROR_MESSAGE = "Could not recover successfully from a failed disk `Block` decompress!"
        self._check_open_raise("decompress")
        self._check_readwrite_raise("decompress")
        check_type(apri, "apri", ApriInfo)
        startn = check_return_int_None_default(startn, "startn", None)
        length = check_return_int_None_default(length, "length", None)
        check_type(ret_metadata, "ret_metadata", bool)

        if startn is not None and startn < 0:
            raise ValueError("`startn` must be non-negative.")

        if length is not None and length < 0:
            raise ValueError("`length` must be non-negative.")

        startn_, length_ = self._resolve_startn_length(apri, startn, length, True)
        blk_key, compressed_key = self._check_blk_compressed_keys_raise(None, None, apri, None, startn_, length_)

        with self._db.begin() as txn:
            compressed_val = txn.get(compressed_key)

        if compressed_val == _IS_NOT_COMPRESSED_VAL:

            raise DecompressionError(
                "The disk `Block` with the following data is not compressed: " +
                f"{str(apri)}, startn = {startn_}, length = {length_}"
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
                blk_filename.unlink()
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

                compressed_filename.unlink()

        except BaseException as e:

            try:

                if not compressed_filename.is_file():
                    raise RegisterRecoveryError(_FAIL_NO_RECOVER_ERROR_MESSAGE)

                elif deleted or not blk_filename.is_file():

                    try:
                        blk_filename.unlink()

                    except FileNotFoundError:
                        pass

                    blk_filename.touch()

            except RegisterRecoveryError as ee:
                raise ee

            except BaseException:
                raise RegisterRecoveryError(_FAIL_NO_RECOVER_ERROR_MESSAGE)

            else:

                if isinstance(e, lmdb.MapFullError):
                    raise RegisterError(_MEMORY_FULL_ERROR_MESSAGE.format(self._db_map_size)) from e

                else:
                    raise e

        if ret_metadata:
            return FileMetadata.from_path(blk_filename)

        else:
            return None

    def is_compressed(self, apri, startn = None, length = None):

        # self._check_open_raise("is_compressed")
        # check_type(apri, "apri", ApriInfo)
        # startn = check_return_int_None_default(startn, "startn", None)
        # length = check_return_int_None_default(length, "length", None)
        # 
        # if startn is not None and startn < 0:
        #     raise ValueError("`startn` must be non-negative.")
        # 
        # if length is not None and length < 0:
        #     raise ValueError("`length` must be non-negative.")

        startn_, length_ = self._resolve_startn_length(apri, startn, length, True)

        with self._db.begin() as txn:
            return txn.get(
                self._get_disk_blk_key(_COMPRESSED_KEY_PREFIX, apri, None, startn_, length_, False, txn)
            ) != _IS_NOT_COMPRESSED_VAL

    # def setHashing(self, info, hashing):
    #     """Enable or disable automatic hashing for disk `Block`s with `info`.
    #
    #     If `hashing` is set to `True`, then every disk `Block` with `info` added to this `Register` will be hashed. A
    #     `Block` is hashed by calling `hash` on each of its entries. The hashes are saved to a hash-set on disk.
    #
    #     If `hashing` is set to `False`, then all hashes associated to `info` will be DELETED (if they exist) and no
    #     future hashes are calculated.
    #
    #     For best results, set hashing to `True` only before adding any disk `Block`s with `info`.
    #
    #     :param hashing: (type `bool`)
    #     """

    #################################
    #    PROTEC DISK BLK METHODS    #

    def _add_disk_blk_txn(self, blk, txn):

        # DEBUG : 6,7,8,9, 10

        apris = [apri for _, apri in blk.apri().iter_inner_info() if isinstance(apri, ApriInfo)]

        if _debug == 6:
            raise KeyboardInterrupt

        # this will create ID's if necessary
        for i, apri in enumerate(apris):
            self._get_id_by_apri(apri, None, True, txn, None)

        if _debug == 7:
            raise KeyboardInterrupt

        blk_key = self._get_disk_blk_key(
            _BLK_KEY_PREFIX,
            blk.apri(), None, blk.startn(), len(blk),
            False, txn
        )

        if not lmdb_has_key(txn, blk_key):

            filename = random_unique_filename(self._local_dir, length=6)

            if _debug == 8:
                raise KeyboardInterrupt

            filename = type(self).with_suffix(filename)

            if _debug == 9:
                raise KeyboardInterrupt

            filename_bytes = str(filename.name).encode("ASCII")
            compressed_key = _COMPRESSED_KEY_PREFIX + blk_key[_BLK_KEY_PREFIX_LEN:]

            txn.put(blk_key, filename_bytes)
            txn.put(compressed_key, _IS_NOT_COMPRESSED_VAL)

            if _debug == 10:
                raise KeyboardInterrupt

            if len(blk) == 0:

                warnings.warn(
                    "Added a length_ 0 disk `Block` to this `Register`.\n" +
                    f"`Register` msg: {str(self)}\n" +
                    f"`Block`: {str(blk)}\n" +
                    f"`Register` location: {str(self._local_dir)}"
                )

            return filename

        else:

            raise RegisterError(
                f"Duplicate `Block` with the following data already exists in this `Register`: " +
                f"{str(blk.apri())}, startn = {blk.startn()}, length = {len(blk)}."
            )

    def _rmv_disk_blk_txn(self, apri, startn, length, txn):

        if _debug == 12:
            raise KeyboardInterrupt

        # raises DataNotFoundError
        self._get_id_by_apri(apri, None, False, txn, None)

        if _debug == 13:
            raise KeyboardInterrupt

        # raises RegisterError and DataNotFoundError
        blk_key, compressed_key = self._check_blk_compressed_keys_raise(None, None, apri, None, startn, length, txn)

        if _debug == 14:
            raise KeyboardInterrupt

        blk_filename, compr_filename = self._check_blk_compressed_files_raise(
            blk_key, compressed_key, apri, startn, length, txn
        )

        if _debug == 15:
            raise KeyboardInterrupt

        txn.delete(compressed_key)

        if _debug == 16:
            raise KeyboardInterrupt

        txn.delete(blk_key)

        if _debug == 17:
            raise KeyboardInterrupt

        return blk_filename, compr_filename

    def _get_disk_blk_key(self, prefix, apri, apri_json, startn, length, missing_ok, txn = None):
        """Get the database key for a disk `Block`.

        One of `info` and `apri_json` can be `None`, but not both. If both are not `None`, then `info` is used.
        `self._db` must be opened by the caller. This method only queries the database to obtain the `info` ID.

        If `missing_ok is True` and an ID for `info` does not already exist, then a new one will be created. If
        `missing_ok is False` and an ID does not already exist, then an error is raised.

        :param prefix: (type `bytes`)
        :param apri: (type `ApriInfo`)
        :param apri_json: (types `bytes`)
        :param startn: (type `int`) The start index of the `Block`.
        :param length: (type `int`) The length_ of the `Block`.
        :param missing_ok: (type `bool`)
        :param txn: (type `lmbd.Transaction`, default `None`) The transaction to query. If `None`, then use open a new
        transaction and commit it after this method resolves.
        :raises DataNotFoundError: If `missing_ok is False` and `info` is not known to this `Register`.
        :return: (type `bytes`)
        """

        if apri is None and apri_json is None:
            raise ValueError

        id_ = self._get_id_by_apri(apri, apri_json, missing_ok, txn, None)
        tail = bytify_num(startn % self._startn_tail_mod, self._startn_tail_length)
        op_length = bytify_num(self._max_length - length, self._length_length)

        return (
                prefix   +
                id_      + _KEY_SEP +
                tail     + _KEY_SEP +
                op_length
        )

    def _num_disk_blks_txn(self, apri, txn):

        try:

            return lmdb_count_keys(
                txn,
                _BLK_KEY_PREFIX + self._get_id_by_apri(apri, None, False, txn, None) + _KEY_SEP
            )

        except DataNotFoundError:
            return 0

    def _iter_disk_blk_pairs(self, prefix, apri, apri_json, txn = None):
        """Iterate over key-value pairs for block entries.

        :param prefix: (type `bytes`)
        :param apri: (type `ApriInfo`)
        :param apri_json: (type `bytes`)
        :param txn: (type `lmbd.Transaction`, default `None`) The transaction to query. If `None`, then use open a new
        transaction and commit it after this method resolves.
        :raise DataNotFoundError: If `apri` is not a disk `ApriInfo`.
        :return: (type `bytes`) key
        :return: (type `bytes`) value
        """

        prefix += self._get_id_by_apri(apri, apri_json, False, txn, None) + _KEY_SEP

        with lmdb_prefix_iter(txn if txn is not None else self._db, prefix) as it:
            yield from it

    def _split_disk_block_key(self, prefix_len, key):

        stop1 = prefix_len + _MAX_NUM_APRI_LENGTH
        stop2 = stop1 + _KEY_SEP_LEN + self._startn_tail_length

        return (
            key[prefix_len           : stop1], # apri id
            key[stop1 + _KEY_SEP_LEN : stop2], # startn
            key[stop2 + _KEY_SEP_LEN : ] # op_length
        )

    @staticmethod
    def _join_disk_block_data(prefix, apri_id, startn_bytes, len_bytes):
        return (
                prefix +
                apri_id + _KEY_SEP +
                startn_bytes + _KEY_SEP +
                len_bytes
        )

    def _convert_disk_block_key(self, prefix_len, key, apri = None, txn = None):
        """
        :param prefix_len: (type `int`) Positive.
        :param key: (type `bytes`)
        :param apri: (type `ApriInfo`, default None) If `None`, the relevant `info` is acquired through a database
        query.
        :param txn: (type `lmbd.Transaction`, default `None`) The transaction to query. If `None`, then use open a new
        transaction and commit it after this method resolves.
        :return: (type `ApriInfo`)
        :return (type `int`) startn
        :return (type `int`) length, non-negative
        """

        apri_id, startn_bytes, op_length_bytes = self._split_disk_block_key(prefix_len, key)

        commit = apri is None and txn is None

        if commit:
            txn = self._db.begin()

        try:

            if apri is None:

                apri_json = self._get_apri_json_by_id(apri_id, txn)
                apri = relational_decode_info(self, ApriInfo, apri_json, txn)

        finally:

            if commit:
                txn.commit()

        try:
            return (
                apri,
                intify_bytes(startn_bytes) + self._startn_head * self._startn_tail_mod,
                self._max_length - intify_bytes(op_length_bytes)
            )
        except ValueError:
            raise

    def _check_blk_compressed_keys_raise(self, blk_key, compressed_key, apri, apri_json, startn, length, txn = None):

        if compressed_key is None and blk_key is None:
            compressed_key = self._get_disk_blk_key(_COMPRESSED_KEY_PREFIX, apri, apri_json, startn, length, False, txn)

        if blk_key is not None and compressed_key is None:
            compressed_key = _COMPRESSED_KEY_PREFIX + blk_key[_BLK_KEY_PREFIX_LEN:]

        elif compressed_key is not None and blk_key is None:
            blk_key = _BLK_KEY_PREFIX + compressed_key[_COMPRESSED_KEY_PREFIX_LEN:]

        commit = txn is None

        if commit:
            txn = self._db.begin()

        try:

            if apri is None:
                apri = relational_decode_info(self, ApriInfo, apri_json, txn)

            has_blk_key = lmdb_has_key(txn, blk_key)
            has_compr_key = lmdb_has_key(txn, compressed_key)

        finally:

            if commit:
                txn.commit()

        if (not has_blk_key and has_compr_key) or (has_blk_key and not has_compr_key):
            raise RegisterError("Uncompressed/compressed `Block` key mismatch.")

        if not has_blk_key:
            raise DataNotFoundError(
                _blk_not_found_err_msg(True, apri, None, startn, length)
            )

        return blk_key, compressed_key

    def _check_blk_compressed_files_raise(self, blk_key, compressed_key, apri, startn, length, txn = None):

        commit = txn is None

        if commit:
            txn = self._db.begin()

        try:

            blk_val = txn.get(blk_key)
            compressed_val = txn.get(compressed_key)
            blk_filename = self._local_dir / blk_val.decode("ASCII")

            if compressed_val != _IS_NOT_COMPRESSED_VAL:

                compressed_filename = self._local_dir / compressed_val.decode("ASCII")

                if not compressed_filename.exists() or not blk_filename.exists():

                    raise RegisterError(
                        "Compressed `Block` file or ghost file seems to be missing!"
                    )

                return blk_filename, compressed_filename

            else:

                if not blk_filename.exists():

                    raise RegisterError(
                        "`Block` file seems to be missing!"
                    )

                return blk_filename, None

        finally:

            if commit:
                txn.commit()

    #################################
    #    PUBLIC RAM BLK METHODS     #

    def add_ram_blk(self, blk):

        self._check_open_raise("add_ram_blk")
        check_type(blk, "blk", Block)

        if blk.apri() not in self._ram_blks.keys():
            self._ram_blks[blk.apri()] = [blk]

        elif len(self._ram_blks[blk.apri()]) == 0:
            self._ram_blks[blk.apri()].append(blk)

        else:

            for i, blk_ in enumerate(self._ram_blks[blk.apri()]):

                if blk_ is blk:
                    break

                elif blk.startn() < blk_.startn() or (blk.startn() == blk_.startn() and len(blk) > len(blk_)):

                    self._ram_blks[blk.apri()].insert(i, blk)
                    break

            else:
                self._ram_blks[blk.apri()].append(blk)

    def rmv_ram_blk(self, blk):

        self._check_open_raise("add_ram_blk")
        check_type(blk, "blk", Block)

        if blk.apri() not in self._ram_blks.keys():
            raise DataNotFoundError(f"No RAM `Block` found with the following data: {str(blk.apri())}.")

        for i, blk_ in enumerate(self._ram_blks[blk.apri()]):

            if blk_ is blk:

                del self._ram_blks[blk.apri()][i]

                if len(self._ram_blks[blk.apri()]) == 0:
                    del self._ram_blks[blk.apri()]

                return

        else:
            raise DataNotFoundError(f"No matching RAM disk `Block` found.")

    def rmv_all_ram_blks(self):

        self._check_open_raise("rmv_all_ram_blks")
        self._ram_blks = {}

    #################################
    #    PROTEC RAM BLK METHODS     #

    #################################
    # PUBLIC RAM & DISK BLK METHODS #

    def blk_by_n(self, apri, n, diskonly = False, recursively = False, ret_metadata = False, **kwargs):

        self._check_open_raise("blk_by_n")
        check_type(apri, "apri", ApriInfo)
        n = check_return_int(n, "n")
        check_type(diskonly, "diskonly", bool)
        check_type(recursively, "recursively", bool)
        check_type(ret_metadata, "ret_metadata", bool)

        if n < 0:
            raise IndexError("`n` must be non-negative.")

        try:
            self._check_known_apri(apri)

        except DataNotFoundError:

            if not recursively:
                raise

        else:

            for startn, length in self.intervals(apri, combine = False, diskonly = diskonly, recursively = False):

                if startn <= n < startn + length:
                    return self.blk(apri, startn, length, diskonly, False, ret_metadata, **kwargs)

        if recursively:

            for subreg in self._iter_subregs():

                with subreg._recursive_open(True) as subreg:

                    try:
                        return subreg.blk_by_n(apri, n, diskonly, True, ret_metadata, **kwargs)

                    except DataNotFoundError:
                        pass

        raise DataNotFoundError(_blk_not_found_err_msg(diskonly, apri, n))

    def blk(self, apri, startn = None, length = None, diskonly = False, recursively = False, ret_metadata = False, **kwargs):

        self._check_open_raise("blk")

        check_type(apri, "apri", ApriInfo)
        startn = check_return_int_None_default(startn, "startn", None)
        length = check_return_int_None_default(length, "length", None)
        check_type(diskonly, "diskonly", bool)
        check_type(recursively, "recursively", bool)
        check_type(ret_metadata, "ret_metadata", bool)

        if startn is not None and startn < 0:
            raise ValueError("`startn` must be non-negative.")

        if length is not None and length < 0:
            raise ValueError("`length` must be non-negative.")

        try:
            self._check_known_apri(apri)

        except DataNotFoundError:

            if not recursively:
                raise

        else:

            startn_, length_ = self._resolve_startn_length(apri, startn, length, diskonly)

            if not diskonly and apri in self._ram_blks.keys():

                for blk in self._ram_blks[apri]:

                    if blk.startn() == startn_ and len(blk) == length_:

                        if ret_metadata:
                            return blk, None

                        else:
                            return blk

            try:
                blk_key, compressed_key = self._check_blk_compressed_keys_raise(None, None, apri, None, startn_, length_)

            except DataNotFoundError:
                pass

            else:

                with self._db.begin() as txn:

                    if txn.get(compressed_key) != _IS_NOT_COMPRESSED_VAL:
                        raise CompressionError(
                            "Could not load disk `Block` with the following data because the `Block` is compressed. "
                            "Please call the `Register` method `decompress` first before loading the data.\n" +
                            f"{apri}, startn = {startn_}, length = {length_}"
                        )

                blk_filename, _ = self._check_blk_compressed_files_raise(blk_key, compressed_key, apri, startn_, length_)
                blk_filename = self._local_dir / blk_filename
                data = type(self).load_disk_data(blk_filename, **kwargs)
                blk = Block(data, apri, startn_)

                if ret_metadata:
                    return blk, FileMetadata.from_path(blk_filename)

                else:
                    return blk

        if recursively:

            for subreg in self._iter_subregs():

                with subreg._recursive_open(True) as subreg:

                    try:
                        return subreg.blk(apri, startn, length, ret_metadata, ret_metadata=True)

                    except DataNotFoundError:
                        pass

        raise DataNotFoundError(
            _blk_not_found_err_msg(diskonly, str(apri), None, startn, length)
        )

    def blks(self, apri, diskonly = False, recursively = False, ret_metadata = False, **kwargs):

        self._check_open_raise("blks")
        check_type(apri, "apri", ApriInfo)
        check_type(diskonly, "diskonly", bool)
        check_type(recursively, "recursively", bool)
        check_type(ret_metadata, "ret_metadata", bool)

        try:
            self._check_known_apri(apri)

        except DataNotFoundError:

            if not recursively:
                raise

        else:

            for startn, length in self.intervals(apri, combine = False, diskonly = diskonly, recursively = recursively):

                try:
                    yield self.blk(apri, startn, length, diskonly, False, ret_metadata, **kwargs)

                except DataNotFoundError:
                    pass

        if recursively:

            for subreg in self._iter_subregs():

                with subreg._recursive_open(True) as subreg:
                    yield from subreg.blks(apri, diskonly, True, ret_metadata, **kwargs)

    def __getitem__(self, apri_n_diskonly_recursively):
        return self.get(*Register._resolve_apri_n_diskonly_recursively(apri_n_diskonly_recursively))

    def get(self, apri, n, diskonly = False, recursively = False, **kwargs):

        self._check_open_raise("get")

        if not isinstance(apri, ApriInfo):
            raise TypeError("The first argument to `reg[]` must be an `ApriInfo.")

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

        if not isinstance(diskonly, bool):
            raise TypeError("The second argument of `reg[]` must be of type `bool`.")

        if not isinstance(recursively, bool):
            raise TypeError("The third argument of `reg[]` must be of type `bool`.")

        if isinstance(n, slice):

            if n.start is not None and n.start < 0:
                raise ValueError("Start index cannot be negative.")

            if n.stop is not None and n.stop < 0:
                raise ValueError("Stop index cannot be negative.")

        try:
            self._check_known_apri(apri)

        except DataNotFoundError:

            if not recursively:
                raise

        else:

            if isinstance(n, slice):
                # return iterator if given slice
                return _ElementIter(self, apri, n, diskonly, recursively, kwargs)

            else:

                blk = self.blk_by_n(apri, n, diskonly, recursively, False, **kwargs)
                ret = blk[n]

                if isinstance(blk, ReleaseBlock):
                    blk.release()

                return ret

        if recursively:

            for subreg in self._iter_subregs():

                with subreg._recursive_open(True) as subreg:

                    try:
                        return subreg.get(apri, n, diskonly, recursively, **kwargs)

                    except DataNotFoundError:
                        pass

        raise DataNotFoundError(
            _blk_not_found_err_msg(diskonly, str(apri), n)
        )

    def __setitem__(self, apri_n_diskonly_recursively, value):

        apri, n, diskonly, recursively = Register._resolve_apri_n_diskonly_recursively(apri_n_diskonly_recursively)
        self.set(apri, n, value, diskonly, recursively)

    def set(self, apri, n, value, diskonly = False, recursively = False, **kwargs):

        check_type(apri, "apri", ApriInfo)

        if isinstance(n, slice):
            raise NotImplementedError("support for slices for Register.set coming soon.")

        n = check_return_int(n, "n")
        check_type(diskonly, "diskonly", bool)
        check_type(recursively, "recursively", bool)

        if n < 0:
            raise ValueError("`n` must be non-negative.")

        if not diskonly:

            for blk in self._ram_blks:

                if n in blk:
                    blk[n] = value
                    return

        try:
            blk = self.blk_by_n(apri, n, diskonly, False, False, **kwargs)

        except DataNotFoundError:
            pass

        else:

            blk[n] = value
            self.rmv_disk_blk(apri, blk.startn(), len(blk), False, False)
            self.add_disk_blk(blk)
            return

        if recursively:

            for subreg in self._iter_subregs():

                with subreg._recursive_open(True) as subreg:

                    try:
                        return subreg.set(apri, n, value, diskonly, recursively, **kwargs)

                    except DataNotFoundError:
                        pass

        raise DataNotFoundError(
            _blk_not_found_err_msg(diskonly, str(apri), n)
        )

    def intervals(self, apri, combine = False, diskonly = False, recursively = False):

        self._check_open_raise("intervals")
        check_type(apri, "apri", ApriInfo)
        check_type(diskonly, "diskonly", bool)
        check_type(recursively, "recursively", bool)

        yield from self._intervals_helper(apri, combine, diskonly, recursively, True)

    def total_len(self, apri, diskonly = False, recursively = False):

        self._check_open_raise("total_len")
        check_type(apri, "apri", ApriInfo)
        check_type(diskonly, "diskonly", bool)
        check_type(recursively, "recursively", bool)

        try:
            return sum(t[1] for t in self.intervals(apri, combine = True, diskonly = diskonly, recursively = recursively))

        except DataNotFoundError:
            return 0

    def num_blks(self, apri, diskonly = False, recursively = False):

        self._check_open_raise("num_blks")

        check_type(apri, "apri", ApriInfo)
        check_type(diskonly, "diskonly", bool)
        check_type(recursively, "recursively", bool)

        try:
            self._check_known_apri(apri)

        except DataNotFoundError:

            if not recursively:
                return 0

        else:

            with self._db.begin() as txn:
                ret = self._num_disk_blks_txn(apri, txn)

            if not diskonly:
                ret += sum(len(val) for val in self._ram_blks.values())

        if recursively:

            for subreg in self._iter_subregs():

                with subreg._recursive_open(True) as subreg:
                    ret += subreg.num_blks(apri, diskonly, True)


        return ret

    def maxn(self, apri, diskonly = False, recursively = False):

        self._check_open_raise("maxn")
        check_type(apri, "apri", ApriInfo)
        check_type(diskonly, "diskonly", bool)
        check_type(recursively, "recursively", bool)
        ret = -1

        try:
            self._check_known_apri(apri)

        except DataNotFoundError:

            if not recursively:
                raise

        else:

            for startn, length in self.intervals(apri, combine = False, diskonly = diskonly, recursively = recursively):
                ret = startn + length - 1

        if recursively:

            for subreg in self._iter_subregs():

                with subreg._recursive_open(True) as subreg:

                    try:
                        ret = max(ret, subreg.maxn(apri, diskonly = diskonly, recursively = True))

                    except DataNotFoundError:
                        pass

        if ret == -1:
            raise DataNotFoundError(_blk_not_found_err_msg(diskonly, apri))

        else:
            return ret

    def contains_index(self, apri, index, diskonly = False, recursively = False):

        check_type(apri, "apri", ApriInfo)
        index = check_return_int(index, "index")
        check_type(diskonly, "diskonly", bool)
        check_type(recursively, "recursively", bool)

        if index < 0:
            raise ValueError("`index` must be non-negative.")

        for startn, length in self.intervals(apri, False, diskonly, recursively):

            if startn <= index < startn + length:
                return True

        else:
            return False

    def contains_interval(self, apri, startn, length, diskonly = False, recursively = False):

        check_type(apri, "apri", ApriInfo)
        startn = check_return_int(startn, "startn")
        length = check_return_int(length, "length")
        check_type(diskonly, "diskonly", bool)
        check_type(recursively, "recursively", bool)

        int1 = (startn, length)

        for int2 in self.intervals(apri, True, diskonly, recursively):

            if intervals_overlap(int1, int2):
                return intervals_subset(int1, int2)

        else:
            return False

    #################################
    # PROTEC RAM & DISK BLK METHODS #

    @staticmethod
    def _resolve_apri_n_diskonly_recursively(apri_n_diskonly_recursively):

        if not isinstance(apri_n_diskonly_recursively, tuple) or len(apri_n_diskonly_recursively) <= 1:
            raise TypeError("Must pass at least two arguments to `reg[]`.")

        if len(apri_n_diskonly_recursively) >= 5:
            raise TypeError("Must pass at most four arguments to `reg[]`.")

        if len(apri_n_diskonly_recursively) == 2:

            apri, n = apri_n_diskonly_recursively
            diskonly = False
            recursively = False

        elif len(apri_n_diskonly_recursively) == 3:

            apri, n, diskonly = apri_n_diskonly_recursively
            recursively = False

        else:
            apri, n, diskonly, recursively = apri_n_diskonly_recursively

        return apri, n, diskonly, recursively

    def _resolve_startn_length(self, apri, startn, length, diskonly, txn = None):
        """
        :param apri: (type `ApriInfo`)
        :param startn: (type `int` or `NoneType`) Non-negative.
        :param length: (type `int` or `NoneType`) Positive.
        :raise DataNotFoundError: If `info` is not known to this register, or if no data is found matching startn and
        length.
        :raise ValueError: If `startn_ is None and length_ is not None`.
        :return: (type `int`) Resolved `startn`, always `int`.
        :return: (type `int`) Resolved `length`, always `int`.
        """

        if startn is None and length is not None:
            raise ValueError(f"If you specify a `Block` length, you must also specify a `startn`.")

        elif startn is not None and length is not None:
            return startn, length

        if not diskonly and apri in self._ram_blks.keys():

            if startn is not None and length is None:

                for blk in self._ram_blks[apri]:

                    if blk.startn() == startn:
                        return startn, len(blk)

            else:
                return self._ram_blks[apri][0].startn(), len(self._ram_blks[apri][0])

        if startn is not None and length is None:

            key = self._get_disk_blk_key(_BLK_KEY_PREFIX, apri, None, startn, 1, False, txn) # raises DataNotFoundError
            first_key_sep_index = key.find(_KEY_SEP)
            second_key_sep_index = key.find(_KEY_SEP, first_key_sep_index + 1)
            prefix = key [ : second_key_sep_index + 1]

            with lmdb_prefix_iter(txn if txn is not None else self._db, prefix) as it:

                for key, _ in it:
                    return self._convert_disk_block_key(_BLK_KEY_PREFIX_LEN, key, apri, None)[1:]

                else:
                    raise DataNotFoundError(
                        _blk_not_found_err_msg(True, apri, None, startn, None)
                    )

        else:

            prefix = _BLK_KEY_PREFIX + self._get_id_by_apri(apri, None, False, txn, None) + _KEY_SEP

            with lmdb_prefix_iter(self._db, prefix) as it:

                for key, _ in it:
                    return self._convert_disk_block_key(_BLK_KEY_PREFIX_LEN, key, apri, None)[1:]

                else:
                    raise DataNotFoundError(_blk_not_found_err_msg(True, apri))

    def _intervals_helper(self, apri, combine, diskonly, recursively, root_call):

        if not combine:

            with self._db.begin() as txn:

                try:
                    self._check_known_apri(apri, txn)

                except DataNotFoundError:

                    if not recursively:
                        raise

                else:

                    if not diskonly and apri in self._ram_blks.keys():

                        for blk in self._ram_blks[apri]:
                            yield blk.startn(), len(blk)

                    try:

                        for key, _ in self._iter_disk_blk_pairs(_BLK_KEY_PREFIX, apri, None, txn):
                            yield self._convert_disk_block_key(_BLK_KEY_PREFIX_LEN, key, apri, txn)[1:]

                    except DataNotFoundError:
                        pass

            if recursively:

                for subreg in self._iter_subregs():

                    with subreg._recursive_open(True) as subreg:
                        yield from subreg._intervals_helper(apri, combine, diskonly, recursively, False)

        if combine and root_call:

            if (diskonly or len(self._ram_blks) == 0) and not recursively:
                intervals_sorted = list(self._intervals_helper(apri, False, diskonly, recursively, False))

            else:
                intervals_sorted = sorted(
                    self._intervals_helper(apri, False, diskonly, recursively, False),
                    key = lambda t: (t[0], -t[1])
                )

            ret = []

            for startn, length in intervals_sorted:

                if len(ret) == 0:
                    ret.append((startn, length))

                elif startn <= ret[-1][0] + ret[-1][1]:
                    ret[-1] = (ret[-1][0], max(startn + length - ret[-1][0], ret[-1][1]))

            yield from ret


class PickleRegister(Register):

    @classmethod
    def with_suffix(cls, filename):
        return filename.with_suffix(".pkl")

    @classmethod
    def dump_disk_data(cls, data, filename, **kwargs):

        if len(kwargs) > 0:
            raise KeyError("This method accepts no keyword-arguments.")

        with filename.open("wb") as fh:
            pickle.dump(data, fh)

    @classmethod
    def load_disk_data(cls, filename, **kwargs):

        if len(kwargs) > 0:
            raise KeyError("`Pickle_Register.blk` accepts no keyword-arguments.")

        with filename.open("rb") as fh:
            return pickle.load(fh), filename

class NumpyRegister(Register):

    @classmethod
    def dump_disk_data(cls, data, filename, **kwargs):

        if len(kwargs) > 0:
            raise KeyError("This method accepts no keyword-arguments.")

        np.save(filename, data, allow_pickle = False, fix_imports = False)

    @classmethod
    def load_disk_data(cls, filename, **kwargs):

        if "mmap_mode" in kwargs:
            mmap_mode = kwargs["mmap_mode"]

        else:
            mmap_mode = None

        if len(kwargs) > 1:
            raise KeyError("`Numpy_Register.get_disk_data` only accepts the keyword-argument `mmap_mode`.")

        NumpyRegister._check_mmap_mode_raise(mmap_mode)
        return np.load(filename, mmap_mode = mmap_mode, allow_pickle = False, fix_imports = False)

    @classmethod
    def clean_disk_data(cls, filename, **kwargs):

        if len(kwargs) > 0:
            raise KeyError("This method accepts no keyword-arguments.")

        filename = filename.with_suffix(".npy")
        return Register.clean_disk_data(filename)

    @classmethod
    def with_suffix(cls, filename):
        return filename.with_suffix(".npy")

    @staticmethod
    def _check_mmap_mode_raise(mmap_mode):

        if mmap_mode not in [None, "r+", "r", "w+", "c"]:
            raise ValueError(
                "The keyword-argument `mmap_mode` for `Numpy_Register.blk` can only have the values " +
                "`None`, 'r+', 'r', 'w+', 'c'. Please see " +
                "https://numpy.org/doc/stable/reference/generated/numpy.memmap.html#numpy.memmap for more information."
            )

    def set(self, apri, n, value, diskonly = False, recursively = False, **kwargs):

        check_type(apri, "apri", ApriInfo)

        if isinstance(n, slice):
            raise NotImplementedError("support for slices for NumpyRegister.set coming soon.")

        n = check_return_int(n, "n")
        check_type(diskonly, "diskonly", bool)
        check_type(recursively, "recursively", bool)

        try:
            mmap_mode = kwargs['mmap_mode']

        except KeyError:
            mmap_mode = None

        else:
            del kwargs['mmap_mode']

        if mmap_mode is not None and mmap_mode != "r+":
            raise ValueError("`mmap_mode` can either be `None` or 'r+'.")

        if not diskonly:

            for blk in self._ram_blks:

                if n in blk:
                    blk[n] = value
                    return

        if mmap_mode is None:

            super().set(apri, n, value, diskonly, recursively, **kwargs)
            return

        else:

            try:
                blk = self.blk_by_n(apri, n, diskonly, False, False, mmap_mode = "r+", **kwargs)

            except DataNotFoundError:
                pass

            else:
                blk.segment()[n, ...] = value

        if recursively:

            for subreg in self._iter_subregs():

                with subreg._recursive_open(True) as subreg:

                    try:
                        return subreg.set(apri, n, value, diskonly, recursively, mmap_mode = mmap_mode, **kwargs)

                    except DataNotFoundError:
                        pass

        raise DataNotFoundError(_blk_not_found_err_msg(diskonly, str(apri), n))

    def blk(self, apri, startn = None, length = None, diskonly = False, recursively = False, ret_metadata = False, **kwargs):
        """
        :param apri: (type `ApriInfo`)
        :param startn: (type `int`)
        :param length: (type `length_`) non-negative
        :param ret_metadata: (type `bool`, default `False`) Whether to return a `File_Metadata` object, which
        contains file creation date/time and size of dumped saved on the disk.
        :param recursively: (type `bool`, default `False`) Search all subregisters for the `Block`.
        :param mmap_mode: (type `str`, optional) Load the Numpy file using memory mapping, see
        https://numpy.org/doc/stable/reference/generated/numpy.memmap.html#numpy.memmap for more information.
        :return: (type `File_Metadata`) If `ret_metadata is True`.
        """

        ret = super().blk(apri, startn, length, diskonly, recursively, ret_metadata, **kwargs)

        if ret_metadata:
            blk = ret[0]

        else:
            blk = ret

        if isinstance(blk.segment(), np.memmap):
            blk = MemmapBlock(blk.segment(), blk.apri(), blk.startn())

        if ret_metadata:
            return blk, ret[1]

        else:
            return blk

    def concat_disk_blks(self, apri, startn = None, length = None, delete = False, ret_metadata = False):
        """Concatenate several `Block`s into a single `Block` along axis 0 and save the new one to the disk.

        If `delete = True`, then the smaller `Block`s are deleted automatically.

        The interval `range(startn, startn + length)` must be the disjoint union of intervals of the form
        `range(blk.startn(), blk.startn() + len(blk))`, where `blk` is a disk `Block` with `ApriInfo`
        given by `info`.

        Length-0 `Block`s are ignored.

        If `startn` is not specified, it is taken to be the smallest `startn` of any `Block` saved to this
        `Register`. If `length` is not specified, it is taken to be the length of the largest
        contiguous set of indices that start with `startn`. If `startn` is not specified but `length` is, a
        ValueError is raised.

        :param apri: (type `ApriInfo`)
        :param startn: (type `int`) Non-negative.
        :param length: (type `int`) Positive.
        :param delete: (type `bool`, default `False`)
        :param ret_metadata: (type `bool`, default `False`) Whether to return a `File_Metadata` object, which
        contains file creation date/time and size of dumped dumped to the disk.
        :raise DataNotFoundError: If the union of the intervals of relevant disk `Block`s does not equal
        `range(startn, startn + length)`.
        :raise ValueError: If any two intervals of relevant disk `Block`s intersect.
        :raise ValueError: If any two relevant disk `Block` segments have inequal shapes.
        :return: (type `File_Metadata`) If `ret_metadata is True`.
        """

        _FAIL_NO_RECOVER_ERROR_MESSAGE = "Could not successfully recover from a failed disk `Block` concatenation!"
        self._check_open_raise("concat_disk_blks")
        self._check_readwrite_raise("concat_disk_blks")
        check_type(apri, "apri", ApriInfo)
        startn = check_return_int_None_default(startn, "startn", None)
        length = check_return_int_None_default(length, "length", None)
        check_type(ret_metadata, "ret_metadata", bool)

        if startn is not None and startn < 0:
            raise ValueError("`startn` must be non-negative.")

        if length is not None and length < 0:
            raise ValueError("`length` must be non-negative.")

        self._check_known_apri(apri)
        # infer startn
        startn, _ = self._resolve_startn_length(apri, startn, length, True)
        # this implementation depends on `intervals` returning smaller startn before larger
        # ones and, when ties occur, larger lengths before smaller ones.
        if length is None:
            # infer length

            current_segment = False
            length = 0

            for startn_, length_ in self.intervals(apri, combine = False, diskonly = False):

                if length_ > 0:

                    if current_segment:

                        if startn > startn_:
                            raise RuntimeError("Could not infer a value for `length`.")

                        elif startn == startn_:
                            raise ValueError(
                                f"Overlapping `Block` intervals found with {str(apri)}."
                            )

                        else:

                            if startn + length > startn_:
                                raise ValueError(
                                    f"Overlapping `Block` intervals found with {str(apri)}."
                                )

                            elif startn + length == startn_:
                                length += length_

                            else:
                                break

                    else:

                        if startn < startn_:
                            raise DataNotFoundError(_blk_not_found_err_msg(True, apri, None, startn))

                        elif startn == startn_:

                            length += length_
                            current_segment = True

            if length == 0:
                raise RuntimeError("could not infer a value for `length`.")

            warnings.warn(f"`length` value not specified, inferred value: `length = {length}`.")

        combined_interval = None

        last_check = False
        last_startn_ = None
        startn_ = None
        length_ = None
        intervals_to_get = []

        for startn_, length_ in self.intervals(apri, combine = False, diskonly = True):
            # infer blocks to combine

            if last_check:

                if last_startn_ == startn_ and length_ > 0:
                    raise ValueError(f"Overlapping `Block` intervals found with {str(apri)}.")

                else:
                    break

            if length_ > 0:

                last_startn_ = startn_

                if startn_ < startn:

                    if startn < startn_ + length_:
                        raise ValueError(
                            f"The first `Block` does not have the right size. Try again by calling "
                            f"`reg.concat_disk_blks({str(apri)}, {startn_}, {length - (startn_ - startn)})`."
                        )

                else:

                    if combined_interval is None:

                        if startn_ > startn:

                            raise DataNotFoundError(
                                f"No disk `Block` found with the following data: `{str(apri)}, startn = {startn}`."
                            )

                        elif startn_ == startn:

                            combined_interval = (startn_, length_)
                            intervals_to_get.append((startn_, length_))
                            last_check = startn_ + length_ == startn + length

                        else:
                            raise RuntimeError("Something went wrong trying to combine `Block`s.")

                    else:

                        if startn_ > sum(combined_interval):

                            raise DataNotFoundError(
                                f"No `Block` found covering indices {sum(combined_interval)} through "
                                f"{startn_-1} (inclusive) with {str(apri)}."
                            )

                        elif startn_ == sum(combined_interval):

                            if startn_ + length_ > startn + length:
                                raise ValueError(
                                    f"The last `Block` does not have the right size. Try again by calling "
                                    f"`reg.concat_disk_blks({str(apri)}, {startn}, "
                                    f"{length - (startn_ + length_ - (startn + length))})`."
                                )

                            combined_interval = (startn, combined_interval[1] + length_)
                            intervals_to_get.append((startn_, length_))
                            last_check = startn_ + length_ == startn + length

                        else:
                            raise ValueError(f"Overlapping `Block` intervals found with {str(apri)}.")

        else:

            if startn_ is None:
                raise DataNotFoundError(_blk_not_found_err_msg(True, apri))

            elif startn_ + length_ != startn + length:
                raise ValueError(
                    f"The last `Block` does not have the right size. "
                    f"Try again by calling `reg.concat_disk_blks(info, {startn}, {startn_ + length_})`."
                )

        if len(intervals_to_get) == 1:

            if ret_metadata:
                return self.blk_metadata(apri, *intervals_to_get)

            else:
                return None

        blks = []
        fixed_shape = None
        ref_blk = None
        failure_reinsert_indices = []
        combined_blk = None

        try:

            for startn_, length_ in intervals_to_get:
                # check that blocks have the correct shape

                blk = self.blk(apri, startn_, length_, True, False, False, mmap_mode="r")
                blks.append(blk)

                if fixed_shape is None:

                    fixed_shape = blk.segment().shape[1:]
                    ref_blk = blk

                elif fixed_shape != blk.segment().shape[1:]:

                    raise ValueError(
                        "Cannot combine the following two `Block`s because all axes other than axis 0 must have the same " +
                        "shape:\n" +
                        f"{str(apri)}, startn = {ref_blk.startn()}, length = {len(ref_blk)}\n, shape = " +
                        f"{str(fixed_shape)}\n" +
                        f"{str(apri)}, startn = {startn_}, length = {length_}\n, shape = " +
                        f"{str(blk.segment().shape)}\n"

                    )

            combined_blk = np.concatenate([blk.segment() for blk in blks], axis=0)
            combined_blk = Block(combined_blk, apri, startn)
            ret = self.add_disk_blk(combined_blk, ret_metadata)

            if _debug == 1:
                raise KeyboardInterrupt

            if delete:

                for blk in blks:

                    startn_ = blk.startn()
                    length_ = len(blk)
                    blk.release()
                    self.rmv_disk_blk(apri, startn_, length_, False)
                    failure_reinsert_indices.append((startn_, length_))

                    if _debug == 2:
                        raise KeyboardInterrupt

        except BaseException as e:

            try:

                if combined_blk is not None and isinstance(combined_blk, Block) and delete:

                    for startn_, length_ in failure_reinsert_indices:
                        self.add_disk_blk(combined_blk[startn_: startn_ + length_])

            except BaseException:
                raise RegisterRecoveryError(_FAIL_NO_RECOVER_ERROR_MESSAGE)

            else:
                raise e

        finally:

            for blk in blks:
                blk.release()

        return ret

class _ElementIter:

    def __init__(self, reg, apri, slc, diskonly, recursively, kwargs):

        self.reg = reg
        self.apri = apri
        self.step = slc.step if slc.step else 1
        self.stop = slc.stop
        self.diskonly = diskonly
        self.recursively = recursively
        self.kwargs = kwargs
        self.curr_blk = None
        self.intervals = None
        self.curr_n = slc.start if slc.start else 0

    def update_intervals_calculated(self):

        self.intervals = list(
            self.reg.intervals(self.apri, combine = False, diskonly = self.diskonly, recursively = self.recursively)
        )

    def get_next_blk(self):

        if self.curr_blk is not None and isinstance(self.curr_blk, ReleaseBlock):
            self.curr_blk.release()

        return self.reg.blk_by_n(self.apri, self.curr_n, self.diskonly, self.recursively, False, **self.kwargs)

    def __iter__(self):
        return self

    def __next__(self):

        if self.stop is not None and self.curr_n >= self.stop:
            raise StopIteration

        elif self.curr_blk is None:

            self.update_intervals_calculated()

            if len(self.intervals) == 0:
                raise StopIteration

            self.curr_n = max(self.intervals[0][0], self.curr_n)

            try:
                self.curr_blk = self.get_next_blk()

            except DataNotFoundError:
                raise StopIteration

        elif self.curr_n not in self.curr_blk:

            try:
                self.curr_blk = self.get_next_blk()

            except DataNotFoundError:

                self.update_intervals_calculated()

                for startn, length in self.intervals:

                    if startn > self.curr_n:

                        self.curr_n += math.ceil( (startn - self.curr_n) / self.step ) * self.step
                        break

                else:
                    raise StopIteration

                self.curr_blk = self.get_next_blk()

        ret = self.curr_blk[self.curr_n]
        self.curr_n += self.step
        return ret

class _CopyRegister(Register):

    @classmethod
    def with_suffix(cls, filename):
        return filename

    @classmethod
    def dump_disk_data(cls, data, filename, **kwargs):

        check_type(data, "data", Path)

        if not data.absolute():
            raise ValueError("`data` must be an absolute `Path`.")

        filename = filename.with_suffix(data.suffix)
        shutil.copyfile(data, filename)
        return filename

    @classmethod
    def load_disk_data(cls, filename, **kwargs):
        raise NotImplementedError

    def set_cls_name(self, cls_name):
        write_txt_file(cls_name, self._cls_filepath)

###################
# RELATIONAL INFO #
###################

_RELATIONAL_APRI_PREFIX = (_APRI_ID_KEY_PREFIX + _KEY_SEP).decode("ASCII")
_RELATIONAL_APRI_PREFIX_LEN = len(_RELATIONAL_APRI_PREFIX)

class _RelationalInfoJsonEncoder(_InfoJsonEncoder):

    def __init__(self, *args, **kwargs):

        if len(args) < 2:
            raise RuntimeError("Must give at least two optional args, a `Register` followed by `lmdb.Transaction`.")

        self._reg, self._txn = args[:2]

        if not isinstance(self._reg, Register):
            raise TypeError("The first argument must have type `Register`.")

        if not is_transaction(self._txn):
            raise TypeError("The second argument must have type `lmdb.transaction`.")

        super().__init__(*args[2:], **kwargs)

    def default(self, obj):

        if isinstance(obj, ApriInfo):
            return _RELATIONAL_APRI_PREFIX + self._reg._get_id_by_apri(obj, None, False, self._txn, None).decode("ASCII")

        else:
            return super().default(obj)

class _RelationalInfoJsonDecoder(_InfoJsonDecoder):

    def __init__(self, *args, **kwargs):

        if len(args) < 2:
            raise RuntimeError("Must give at least two optional args, a `Register` followed by `lmdb.Transaction`.")

        self._reg, self._txn = args

        if not isinstance(self._reg, Register):
            raise TypeError("The first argument must have type `Register`.")

        if not is_transaction(self._txn):
            raise TypeError("The second argument must have type `lmdb.transaction`.")

        super().__init__(*args[2:], **kwargs)

    @staticmethod
    def check_return_apri_id(str_):

        check = len(str_) > _RELATIONAL_APRI_PREFIX_LEN and str_[:_RELATIONAL_APRI_PREFIX_LEN] == _RELATIONAL_APRI_PREFIX
        return check, str_[_RELATIONAL_APRI_PREFIX_LEN:].encode("ASCII") if check else None

    def object_hook(self, obj):

        if isinstance(obj, str):

            obj = obj.strip(" \t")

            check_apri, apri_id = _RelationalInfoJsonDecoder.check_return_apri_id(obj)
            check_apos, apos_json = _InfoJsonDecoder.check_return_apos_info_json(obj)

            if check_apri:

                apri_json = self._reg._get_apri_json_by_id(apri_id, self._txn).decode("ASCII")

                try:
                    return ApriInfo(**self.decode(apri_json))

                except:
                    raise

            elif check_apos:
                return AposInfo(**self.decode(apos_json))

            else:
                return obj

        else:
            return super().object_hook(obj)

def relational_encode_info(reg, info, txn = None):

    check_type(reg, "reg", Register)
    check_type(info, "info", _Info)

    if txn is not None and not is_transaction(txn):
        raise TypeError("`txn` must be of type `lmdb.Transaction`.")

    commit = txn is None

    if commit:
        txn = reg._db.begin()

    try:

        encoder = _RelationalInfoJsonEncoder(reg, txn,
            ensure_ascii = True,
            allow_nan = True,
            indent = None,
            separators = (',', ':')
        )
        info.set_encoder(encoder)
        return info.to_json().encode("ASCII")

    finally:

        info.clean_encoder() # necessary so that the garbage collector cleans Transactions properly

        if commit:
            txn.commit()

def relational_decode_info(reg, cls, json, txn = None):

    check_type(reg, "reg", Register)
    check_type(json, "json", bytes)

    if not issubclass(cls, _Info):
        raise TypeError("`cls` must be a subclass of `_Info`.")

    if txn is not None and not is_transaction(txn):
        raise TypeError("`txn` must be of type `lmdb.Transaction`.")

    commit = txn is None

    if commit:
        txn = reg._db.begin()

    try:

        decoder = _RelationalInfoJsonDecoder(reg, txn)
        return cls.from_json(json.decode("ASCII"), decoder)

    finally:

        if commit:
            txn.commit()