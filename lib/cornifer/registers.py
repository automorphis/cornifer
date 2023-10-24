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
import itertools
import json
import pickle
import shutil
import warnings
import zipfile
from contextlib import contextmanager, ExitStack
from pathlib import Path
from abc import ABC, abstractmethod
import time

import lmdb
import numpy as np

from ._utilities.multiprocessing import copytree_with_timeout
from .errors import DataNotFoundError, RegisterAlreadyOpenError, RegisterError, CompressionError, \
    DecompressionError, NOT_ABSOLUTE_ERROR_MESSAGE, RegisterRecoveryError, BlockNotOpenError, DataExistsError, \
    RegisterNotOpenError, RegisterOpenError
from .info import ApriInfo, AposInfo, _InfoJsonEncoder
from .blocks import Block, MemmapBlock
from .filemetadata import FileMetadata
from ._utilities import random_unique_filename, resolve_path, BYTES_PER_MB, is_deletable, check_type, \
    check_return_int_None_default, check_Path, check_return_int, bytify_int, intify_bytes, intervals_overlap, \
    write_txt_file, read_txt_file, intervals_subset, FinalYield, combine_intervals, sort_intervals, is_int, hash_file
from ._utilities.lmdb import r_txn_has_key, open_lmdb, ReversibleWriter,  num_open_readers_accurate, \
    r_txn_prefix_iter, r_txn_count_keys
from .regfilestructure import VERSION_FILEPATH, LOCAL_DIR_CHARS, \
    COMPRESSED_FILE_SUFFIX, MSG_FILEPATH, CLS_FILEPATH, check_reg_structure, DATABASE_FILEPATH, \
    REG_FILENAME, MAP_SIZE_FILEPATH, SHORTHAND_FILEPATH, WRITE_DB_FILEPATH, DATA_FILEPATH, DIGEST_FILEPATH
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

_MEMORY_FULL_ERROR_MESSAGE = (
    "Exceeded max `Register` size of {0} Bytes. Please increase the max size using the method `increase_size`."
)
_NO_APRI_ERROR_MESSAGE = "The following `ApriInfo` is not known to this register :\n{0}\n{1}"
_NO_APOS_ERROR_MESSAGE = "No apos associated with the following apri : \n{0}\n{1}"
_RAM_BLOCK_NOT_OPEN_ERROR_MESSAGE = (
    "Closed RAM `Block` with the following data (it is good practice to always keep all RAM `Block`s open) :\n{0}\n"
    "startn = {1}."
)

#################################
#           CONSTANTS           #

_START_N_TAIL_LENGTH_DEFAULT   = 12
_LENGTH_LENGTH_DEFAULT         = 7
_MAX_LENGTH_DEFAULT            = 10 ** _LENGTH_LENGTH_DEFAULT - 1
_START_N_HEAD_DEFAULT          = 0
_INITIAL_REGISTER_SIZE_DEFAULT = 5 * BYTES_PER_MB
_MAX_NUM_APRI_LEN              = 6
_MAX_NUM_APRI                  = 10 ** _MAX_NUM_APRI_LEN

class Register(ABC):

    file_suffix = ""
    _constructors = {}
    _instances = {}

    #################################
    #            PATTERNS           #

    # I. PUBLIC/PROTECTED METHOD AND CONTEXTMANAGER PATTERNS:
    # 1. A "method" is any instance-, static-, or class-method not decorated by `@contextmanager`. A "contextmanager",
    #    abbreviated "cm", is any instance-, static-, or class-method decorated by `@contextmanager`.
    # 2. Public methods/cm's have no leading underscore `_`. Protected methods/cm's have a leading underscore.
    # 3. Public methods may take optional/keyword arguments, but protected methods always have only positional.
    # 4. Any method that must NOT have a `yield` statement is called a "non-generator method"; any method that MUST
    #    have a `yield` statement is called a "generator method".
    # 5. Public methods/cm's that access the `Register` database have each at most six corresponding protected methods,
    #    the "RAM" method, the "pre" method, the "first disk" method, the "second disk method, the "recursive"
    #    method, and the "error handling" method. These protected methods are indicated by the prefix `_`, followed by
    #    the public method name, followed by the suffixes `_pre`, `_ram`, `_disk`, `_disk2`, `_recursive`, and
    #    `_error`, respectively.
    #    a. Public methods that access the `Register` database should be called only by the user and overriding methods.
    #    b. The execution order of a public methods/cm depends on if it is a reader method/cm or a writer method/cm
    #       (see II.2).
    #    c. Reader methods/cms execute in the following order (some steps may be skipped):
    #       i. Start a timer (see `Register._time`)
    #       ii. Check for any `TypeError`s and `ValueError`s.
    #       iii. Call the RAM method.
    #       iv. Create a reader (see II.1) and pass it to the pre method.
    #       v. If the pre method `raise`s `DataNotFoundError`, then ignore the error and pass the same reader to the
    #          recursive method. If the recursive method returns without error, then so too does the public method/cm.
    #       vi. If the pre method returns without error, then pass the same reader to the first disk method.
    #       vii. If the first disk method returns without error, then pass the same reader to the recursive method. If
    #          recursive method returns without error, then so too does the public method/cm.
    #    d. Writer methods/cms execute in the following order (some steps may be skipped):
    #       i. Start a timer.
    #       ii. Check for any `TypeError`s and `ValueError`s.
    #       iii. Call the RAM method. If the RAM method returns without error, then the public method/cm returns
    #          without error.
    #       iv. If the RAM method raises `DataNotFoundError`, then create a reader (see II.1) and pass it to the pre
    #          method.
    #       v. If the pre method returns, then commit the reader.
    #       vi. Create a writer and pass it to the first disk method.
    #       vii. If the first disk method returns and both the reader and writer commit without error, then call the
    #          second disk method. If the second disk method returns without error, then so too does the public
    #          method/cm.
    #       viii. If either steps vi. or vii. error out, then create a second writer and pass it to the error handling
    #          method.
    #    e. The RAM method reads from and makes changes to RAM `Block`s. A `return` indicates that this read or write
    #       was successful and a `raise DataNotFoundError` indicates, well...
    #    f. The pre method checks for any errors that can be detected by a reader or by readonly OS functions and
    #       returns the inputs to the first and second disk methods. The pre method must be a non-generator.
    #    g. The first disk method reads from and makes all necessary changes to the LMDB database. This method may be
    #       a generator.
    #    h. The second main method saves data outside of the LMDB database; for example, the second main method
    #       of `add_disk_blk` calls the function `dump_disk_data`. This method must be a non-generator.
    #    i. The recursive method runs steps c.iii, c.iv, and c.vi on every subregister.
    #    j. The error handling method attempts to reverse the effects of the function. This method must be a non-
    #       generator.
    #
    #
    # II. `lmdb.Transaction` PATTERNS:
    # 1. A "reader" is a readonly `lmdb.Transaction`. A "writer" is a read/write `lmdb.Transaction` or a
    #    `ReversibleTransaction`. A reader can be created either by `lmdb.Environment.begin` or `Register._reader` and
    #    a writer can be created by `lmdb.Environment.begin`, `Register._reversible_writer`, or
    #    `ReversibleWriter.begin`.
    # 2. A "reader method/cm" is any method/cm that (takes a reader as input OR creates a reader) AND (does NOT take
    #    a writer as input OR creates a writer). A "writer method/cm" is any method/cm that takes a writer as input
    #    OR creates a writer.
    # 3. In most cases, any method/cm that accesses the `Register` database (`_db`) may do so with at most one reader
    #    and at most one writer.
    #    a. The only exception is if an occurs somewhere after committing a writer, in which case a second writer may
    #       be opened to reverse the changes of the first writer (see I.5.d.viii).
    #    b. The readers and writers opened by called methods count toward the caller's ration of readers and writers.
    #       Therefore, `Transaction`s are, generally, created by public methods and passed to protected methods as
    #       parameters, as described in I.5. The public method then commits or aborts the `Transaction`.
    #    c. There may be at most one reader and one writer per method PER REGISTER. You may create readers and writers
    #       that access any subregister database, but each subregister may have at most one reader and writer per
    #       method.
    # 4. A read/write `lmdb.Transaction` has the name `rw_txn` or some variant thereof; a readonly `lmdb.Transaction`
    #    has the name `ro_txn`; a `ReversibleTransaction` has the name `rrw_txn`. If an `lmdb.Transaction` or a
    #    `ReversibleTransaction` is passed as an argument and the method only reads and never writes to the
    #    transaction, then the argument is named `r_txn`.
    #
    #
    # III. `info._Info` PATTERNS
    # 1. In most cases, protected methods that take an `_Info` object as a parameter also take its JSON encoding
    #    (usually either `apri_json` or `apos_json`) and a `bool` named `reencode`.
    #    a. The exceptions are if the method calculates the JSON encoding from the `_Info` object or vice-versa (e.g.
    #       `Register._relational_encode_info`).
    #    b. The JSON encoding is passed as a parameter so that it is not re-calculated from the `_Info` object by
    #       every single method that needs the encoding, unless necessary.
    #    c. The `apri` parameter must always be non-`None`, but the `apri_json` parameter may be `None` if
    #       `reencode is True`.
    #    d. Removed.
    #
    #
    # IV. `Block` PATTERNS
    # 1. Like `info._Info` (see III.1), protected methods that take a `Block` object as a parameter also take two
    #    `bytes` objects named `blk_key` and `compressed_key` and a `bool` named `reencode`.
    #    a. `blk_key, compressed_key` is the return value of
    #           self._get_disk_blk_keys(blk.apri(), None, True, blk.startn(), len(blk), r_txn)
    #
    #
    # V. RECURSIVE PATTERNS
    # 1. A "recursive method/cm" is any public method/cm that takes a optional `bool` argument named "recursively".
    #    a. If `recursively is True`, then the method/cm is also called on subregisters.
    #    b. The method may not be called on all subregisters; for example, the cm `blk` `yield`s as soon as it finds
    #       the requested data, which may be in the root register.
    # 2. Only reader methods/cm's may be recursive, never writers (see II.2).
    # 3. Recursive calls are always breadth-first.
    #    a. Towards this end, the method `Register._subregs_bfs` `yield`s subregisters in a breadth-first manner.
    # 4. Most implementations of recursive non-generator methods are minor variations of the following pattern:
    #
    #        def _methodname_recursive(self, args1, args2, diskonly, r_txn):
    #            ret = None
    #            for subreg, ro_txn in self._subregs_bfs(True, r_txn):
    #                if not diskonly:
    #                    try:
    #                        ret = subreg._methodname_ram(args1)
    #                    except DataNotFoundError:
    #                        pass
    #                if ret is None:
    #                    try:
    #                        args3 = subreg._methodname_pre(args2, ro_txn)
    #                    except DataNotFoundError:
    #                        pass
    #                    else:
    #                        ret = subreg._methodname_disk(args3, ro_txn)
    #                if ret is not None:
    #                    # code to run on `ret`
    #                    return ret
    #            if ret is None:
    #                raise DataNotFoundError
    #
    # 5. Most implementations of recursive generator methods are minor variations of the following pattern:
    #
    #        def _methodname_recursive(self, args1, args2, diskonly, r_txn):
    #            for subreg, ro_txn in self._subregs_bfs(True, r_txn):
    #                if not diskonly:
    #                    yield from subreg._methodname_ram(args1)
    #                try:
    #                    args3 = subreg._methodname_pre(args2, ro_txn)
    #                except DataNotFoundError:
    #                    pass
    #                else:
    #                    yield from subreg._methodname_main(args3, ro_txn)
    #
    #
    # VI. `DataError` PATTERNS
    # 1. Among protected methods, `DataError` and its subtypes may be `raise`d by any RAM, pre, or recursive
    #    method, along with ONLY the following:
    #        _get_apri_id (DataNotFoundError)
    #        _relational_encode_info (DataNotFoundError)
    # 2. `DataNotFoundError` may be `raise`d to the caller of a public, recursive, non-generator method if and only if
    #    the data is not found in the root register or any of its subregisters (or any of their subregisters, etc). In
    #    particular, the non-existence of the requested data on the root register or a single subregister is
    #    insufficient for such an error to be `raise`d.
    # 3. Generator methods never raise and always catch `DataNotFoundError`. Instead, they simply `yield` nothing.
    # 4. If `DataError` is unexpectedly caught, it should be reraised as a `RegisterError`.
    #
    #
    # VII. DISK AND RAM `Block` METHOD PATTERNS
    # 1. A "disk and RAM `Block` method" is any method that accesses both disk and RAM `Block`s; for example, `apris`,
    #    `blk`, `get`, and `set`.
    # 2. In any `Register`, a disk and RAM `Block` method always first accesses RAM `Block`s, second disk `Block`s.
    #    This rule also applies to recursive calls; that is, first RAM `Block`s of the root self, second disk `Block`s
    #    of the root self, third RAM `Block`s of subreg A, fourth disk `Block`s of subreg A, fifth RAM `Block`s of
    #    subreg B, etc.
    #
    #
    # VIII. CONTEXTMANAGER PATTERNS
    # 1. Some contextmanagers may need to be `__exit__`ed in an order that is not opposite to the order they were
    #    `__enter__`ed. To accomplish this we use an `ExitStack`.

    #################################
    #     PUBLIC INITIALIZATION     #

    def __init__(self, dir, shorthand, msg, initial_reg_size = None, use_custom_lock = False, _pickle_data = None):
        """
        :param dir: (type `str`) Directory where this `Register` is saved.
        :param shorthand: (type `str`) A word or short phrase describing this `Register`.
        :param msg: (type `str`) A more detailed message describing this `Register`.
        :param initial_reg_size: (type `int`, default 5242880 (5 MB)) Maximum memory size in bytes. This size is NOT
        the memory needed to store disk `Block`s; rather, it is the memory needed to store disk `Block` metadata,
        `ApriInfo`, and `AposInfo`. You may wish to set this size lower than 5 MB if you do not expect to add much data
        to this `Register`. If your `Register` exceeds `initial_reg_size`, then you can adjust the database size later
        via the method `increase_size`. If you are on a non-Windows system, there is no harm in setting this value to
        be very large (e.g. 1 TB).
        :param use_custom_lock: (type `bool`, default `False`) Experimental.
        """

        self._set_attributes(dir, shorthand, msg, initial_reg_size, use_custom_lock)
        local_dir = random_unique_filename(self.dir, length = 4, alphabet = LOCAL_DIR_CHARS)

        try:
            # set local directory info and create LMDB database
            local_dir.mkdir(exist_ok=False)
            (local_dir / REG_FILENAME).mkdir(exist_ok=False)
            (local_dir / DATABASE_FILEPATH).mkdir(exist_ok=False)
            write_txt_file(self._shorthand, local_dir / SHORTHAND_FILEPATH)
            write_txt_file(self._msg, local_dir / MSG_FILEPATH)
            write_txt_file(self._version, local_dir / VERSION_FILEPATH)
            write_txt_file(str(type(self).__name__), local_dir / CLS_FILEPATH)
            write_txt_file(str(self._db_map_size), local_dir / MAP_SIZE_FILEPATH)
            write_txt_file("",
                           local_dir / WRITE_DB_FILEPATH)  # this file has to exist prior to `_set_local_dir` call
            self._set_local_dir(local_dir)
            write_txt_file(str(self._write_db_filepath), local_dir / WRITE_DB_FILEPATH, True)
            self._db = open_lmdb(self._write_db_filepath, self._db_map_size, False)

            with self._writer() as rw_txn:

                rw_txn.put(_START_N_HEAD_KEY, str(self._startn_head).encode("ASCII"))
                rw_txn.put(_START_N_TAIL_LENGTH_KEY, str(self._startn_tail_length).encode("ASCII"))
                rw_txn.put(_LENGTH_LENGTH_KEY, str(_LENGTH_LENGTH_DEFAULT).encode("ASCII"))
                rw_txn.put(_CURR_ID_KEY, b"0")

            self._db.close()
            Register._add_instance(local_dir, self)

        except BaseException as e:

            if local_dir.exists():
                shutil.rmtree(local_dir)

            raise e

    def __init_subclass__(cls, **kwargs):

        Register._constructors[cls.__name__] = cls
        file_suffix = kwargs.pop("file_suffix", None)

        if file_suffix is not None:

            if not isinstance(file_suffix, str):
                raise TypeError(f"`file_suffix` keyword argument must be of type `str`, not `{type(file_suffix)}`.")

            if len(file_suffix) > 0 and file_suffix[0] != ".":
                file_suffix = "." + file_suffix

            cls.file_suffix = file_suffix

        super().__init_subclass__(**kwargs)

    def __new__(cls, dir, shorthand, msg, initial_reg_size = None, use_custom_lock = False, _pickle_data = None):

        if _pickle_data is not None:
            local_dir = Path(_pickle_data)

        else:
            local_dir = None

        if local_dir is not None and Register._instance_exists(local_dir):
            return Register._get_instance(local_dir)

        else:
            return super().__new__(cls)

    #################################
    #     PROTEC INITIALIZATION     #

    def _set_attributes(self, dir, shorthand, msg, initial_reg_size, use_custom_lock):

        check_Path(dir, "dir")
        check_type(shorthand, "shorthand", str)
        check_type(msg, "msg", str)
        initial_reg_size = check_return_int_None_default(
            initial_reg_size, "initial_reg_size", _INITIAL_REGISTER_SIZE_DEFAULT
        )

        if initial_reg_size <= 0:
            raise ValueError("`initial_reg_size` must be positive.")

        self.dir = resolve_path(Path(dir))

        if not self.dir.is_dir():
            raise NotADirectoryError(f"The path `{str(self.dir)}` exists but is not a directory.")

        # DATABASE #
        self._db = None
        self._msg_filepath = None # set by `Register._set_local_dir`
        self._shorthand_filepath = None # ditto
        self._local_dir = None # ditto
        self._local_dir_bytes = None # ditto
        self._subreg_bytes = None # ditto
        self._perm_db_filepath = None # ditto
        self._write_db_filepath = None # ditto
        self._db_map_size = initial_reg_size
        self._db_map_size_filepath = None # ditto
        self._cls_filepath = None # ditto
        self._digest_filepath = None # ditto
        self._use_custom_lock = use_custom_lock
        # ATTRIBUTES
        self._shorthand = shorthand
        self._readonly = None
        self._msg = msg
        self._opened = False
        # VERSION #
        self._version = CURRENT_VERSION
        self._version_filepath = None
        # INDICES #
        self._startn_head = _START_N_HEAD_DEFAULT
        self._startn_tail_length = _START_N_TAIL_LENGTH_DEFAULT
        self._startn_tail_mod = 10 ** self._startn_tail_length
        self._length_length = _LENGTH_LENGTH_DEFAULT
        self._max_length = _MAX_LENGTH_DEFAULT
        # RAM BLOCKS #
        self._ram_blks = {}
        # TIMEIT #
        self.set_elapsed = 0
        self.get_elapsed = 0
        self.add_elapsed = 0
        self.load_elapsed = 0
        self.rmv_elapsed = 0
        self.compress_elapsed = 0
        self.decompress_elapsed = 0
        # TRANSACTIONS #
        self._do_manage_txn = False
        self._num_active_txns = None
        self._txn_wait_event = None
        self._txn_wait_timeout = None

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
                    f"Cornifer is not aware of a `Register` subclass called `{cls_name}`. Please be sure that "
                    f"`{cls_name}` properly subclasses `Register` and that `{cls_name}` is in the namespace by "
                    f"importing it."
                )

            shorthand = read_txt_file(local_dir / SHORTHAND_FILEPATH)
            msg = read_txt_file(local_dir / MSG_FILEPATH)
            map_size = int(read_txt_file(local_dir / MAP_SIZE_FILEPATH))
            reg = object.__new__(con)
            reg._set_attributes(local_dir.parent, shorthand, msg, map_size, False)
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
    #           PICKLING            #

    def __getstate__(self):
        return {"local_dir" : str(self._local_dir)}

    def __setstate__(self, state):

        local_dir = Path(state["local_dir"])

        if Register._instance_exists(local_dir):
            return

        else:
            self.__dict__ = Register._from_local_dir(local_dir).__dict__

    def __getnewargs__(self):
        return None, None, None, None, None, str(self._local_dir)

    #################################
    #         TRANSACTIONS          #

    @contextmanager
    def _manage_txn(self):

        if self._do_manage_txn:

            self._txn_wait_event.wait(timeout = self._txn_wait_timeout)

            with self._num_active_txns.get_lock():
                self._num_active_txns.value += 1

        try:
            yield

        finally:

            if self._do_manage_txn:

                with self._num_active_txns.get_lock():
                    self._num_active_txns.value -= 1

    @contextmanager
    def _reversible_writer(self):

        with self._manage_txn():

            with ReversibleWriter(self._db).begin() as rrw_txn:
                yield rrw_txn

    @contextmanager
    def _writer(self):

        with self._manage_txn():

            with self._db.begin(write = True) as rw_txn:
                yield rw_txn

    @contextmanager
    def _reader(self):

        with self._manage_txn():

            with self._db.begin() as ro_txn:
                yield ro_txn

    def _set_txn_shared_data(self, num_active_txns, txn_wait_event):

        self._do_manage_txn = True
        self._num_active_txns = num_active_txns
        self._txn_wait_event = txn_wait_event

    #################################
    #    PUBLIC REGISTER METHODS    #

    def __eq__(self, other):


        if type(self) != type(other):
            return False

        else:
            return self._local_dir == other._local_dir

    def __hash__(self):
        return hash(str(self._local_dir)) + hash(type(self))

    def __str__(self):
        return f'{self._shorthand} ({self._local_dir}): {self._msg}'

    def __repr__(self):
        return f'{self.__class__.__name__}("{str(self.dir)}", "{self._shorthand}", "{self._msg}", {self._db_map_size})'

    def set_shorthand(self, shorthand):

        check_type(shorthand, "shorthand", str)
        self._check_open_raise("set_shorthand")
        self._check_readwrite_raise("set_shorthand")
        write_txt_file(shorthand, self._shorthand_filepath, True)
        self._shorthand = shorthand

    def set_msg(self, message, append = False):
        """Give this `Register` a detailed description.

        :param message: (type `str`)
        :param append: (type `bool`, default `False`) Set to `True` to append the new message to the old one.
        """

        check_type(message, "message", str)
        check_type(append, "append", bool)
        self._check_open_raise("set_msg")
        self._check_readwrite_raise("set_msg")

        if append:
            new_msg = self._msg + message

        else:
            new_msg = message

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

        self._check_open_raise("set_startn_info")
        self._check_readwrite_raise("set_startn_info")
        head = check_return_int_None_default(head, "head", _START_N_HEAD_DEFAULT)
        tail_len = check_return_int_None_default(tail_len, "tail_len", _START_N_TAIL_LENGTH_DEFAULT)

        if head < 0:
            raise ValueError("`head` must be non-negative.")

        if tail_len <= 0:
            raise ValueError("`tail_len` must be positive.")

        if head == self._startn_head and tail_len == self._startn_tail_length:
            return

        with self._reader() as ro_txn:
            changes = self._set_startn_info_pre(head, tail_len, ro_txn)

        with self._writer() as rw_txn:
            self._set_startn_info_disk(head, tail_len, changes, rw_txn)

        self._startn_head = head
        self._startn_tail_length = tail_len
        self._startn_tail_mod = 10 ** self._startn_tail_length

    @contextmanager
    def open(self, readonly = False):

        yield_ = self._open(readonly)

        try:
            yield yield_

        finally:
            yield_._close()

    def increase_size(self, num_bytes):
        """WARNING: DO NOT CALL THIS METHOD FROM MORE THAN ONE PYTHON PROCESS AT A TIME. You are safe if you call it
        from only one Python process. You are safe if you have multiple Python processes running and call it from only
        ONE of them. But do NOT call it from multiple processes at once. Doing so may result in catastrophic loss of
        data.

        :param num_bytes: (type `int`) Positive.
        """

        self._check_open_raise("increase_size")
        self._check_readwrite_raise("increase_size")
        num_bytes = check_return_int(num_bytes, "num_bytes")

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
        return str(self._local_dir)

    def shorthand(self):
        return self._shorthand

    def reset_timers(self):

        self.set_elapsed = 0
        self.get_elapsed = 0
        self.add_elapsed = 0
        self.load_elapsed = 0
        self.compress_elapsed = 0
        self.decompress_elapsed = 0

    @contextmanager
    def tmp_db(self, tmp_dir, timeout = None):

        self._check_not_open_raise("make_tmp_db")
        new_write_db_filepath = random_unique_filename(tmp_dir)
        self._write_db_filepath = new_write_db_filepath

        try:

            write_txt_file(str(self._write_db_filepath), self._local_dir / WRITE_DB_FILEPATH, True)

            try:
                shutil.copytree(self._perm_db_filepath, self._write_db_filepath)

            except:

                shutil.rmtree(new_write_db_filepath, ignore_errors = True) # copytree could partially write
                write_txt_file(str(self._perm_db_filepath), self._local_dir / WRITE_DB_FILEPATH, True)
                raise

        except:

            self._write_db_filepath = self._perm_db_filepath
            raise

        try:
            yield self

        finally:

            self._write_db_filepath = self._perm_db_filepath
            write_txt_file(str(self._write_db_filepath), self._local_dir / WRITE_DB_FILEPATH, True)
            self.update_perm_db(timeout)

    def update_perm_db(self, timeout = None):

        file = Path.home() / "parallelize.txt"
        start = time.time()
        self._check_not_open_raise("update_perm_db")
        tmp_filename = self._perm_db_filepath.parent / (DATABASE_FILEPATH.name + "_tmp")
        with file.open("a") as fh:
            fh.write("9.1\n")

        if tmp_filename.exists():
            shutil.rmtree(tmp_filename)

        with file.open("a") as fh:
            fh.write("9.2\n")

        if timeout is not None:
            with file.open("a") as fh:
                fh.write("9.3\n")
            complete = copytree_with_timeout(timeout + start - time.time(), self._write_db_filepath, tmp_filename)

        else:

            shutil.copytree(self._write_db_filepath, tmp_filename)
            with file.open("a") as fh:
                fh.write("9.4\n")

            complete = True

        if complete:

            with file.open("a") as fh:
                fh.write("9.5\n")

            shutil.rmtree(self._perm_db_filepath)
            with file.open("a") as fh:
                fh.write("9.6\n")
            tmp_filename.rename(self._perm_db_filepath)
            with file.open("a") as fh:
                fh.write("9.7\n")
            write_txt_file(self._digest(), self._digest_filepath, True)
            with file.open("a") as fh:
                fh.write("9.8\n")

        else:
            with file.open("a") as fh:
                fh.write("9.9\n")
            shutil.rmtree(tmp_filename)
            with file.open("a") as fh:
                fh.write("9.10\n")

    #################################
    #    PROTEC REGISTER METHODS    #

    def _digest(self):

        ret = hash_file(self._version_filepath)
        hash_file(self._shorthand_filepath, ret)
        hash_file(self._msg_filepath, ret)
        hash_file(self._cls_filepath, ret)
        hash_file(self._db_map_size_filepath, ret)
        hash_file(self._perm_db_filepath / DATA_FILEPATH.name, ret)
        return ret.hexdigest()

    def _approx_memory(self):
        # use only for debugging
        stat = self._db.stat()
        current_size = stat['psize'] * (stat['leaf_pages'] + stat['branch_pages'] + stat['overflow_pages'] )

        with self._reader() as ro_txn:

            with r_txn_prefix_iter(b"", ro_txn) as it:
                entry_size_bytes = sum(len(key) + len(value) for key, value in it) * 1

        return current_size + entry_size_bytes

    def _set_startn_info_pre(self, head, tail_len, r_txn):

        new_mod = 10 ** tail_len

        for apri, apri_json in self._apris_disk(r_txn):

            prefix = self._intervals_pre(apri, apri_json, False, r_txn)

            for startn, length in self._intervals_disk(prefix, r_txn):

                if startn // new_mod != head:
                    raise ValueError(
                        "The following startn does not have the correct head:\n"
                        f"startn   : {startn}\n"
                        "That startn is associated with a `Block` whose apri and length is:\n"
                        f"ApriInfo : {apri}\n"
                        f"length   : {length}\n"
                    )

        changes = []

        for prefix, prefix_len in (
            (_BLK_KEY_PREFIX, _BLK_KEY_PREFIX_LEN), (_COMPRESSED_KEY_PREFIX, _COMPRESSED_KEY_PREFIX_LEN)
        ):

            with r_txn_prefix_iter(prefix, r_txn) as it:

                for key, val in it:

                    startn, _ = self._get_startn_length(prefix_len, key)
                    apri_id, _, length_bytes = self._get_raw_startn_length(prefix_len, key)
                    new_startn_bytes = bytify_int(startn % new_mod, tail_len)
                    new_key = Register._join_disk_blk_data(
                        _BLK_KEY_PREFIX, apri_id, new_startn_bytes, length_bytes
                    )

                    if key != new_key:

                        changes.append((new_key, val))
                        changes.append((key, None))

        return changes

    @staticmethod
    def _set_startn_info_disk(head, tail_len, changes, rw_txn):

        rw_txn.put(_START_N_HEAD_KEY, bytify_int(head))
        rw_txn.put(_START_N_TAIL_LENGTH_KEY, bytify_int(tail_len))

        for key, val in changes:

            if val is not None:
                rw_txn.put(key, val)

            else:
                rw_txn.delete(key)

    def _open(self, readonly):

        if Register._instance_exists(self._local_dir):
            ret = Register._get_instance(self._local_dir)

        else:
            ret = self

        if ret._db is not None and ret._opened:
            raise RegisterAlreadyOpenError(self)

        ret._readonly = readonly
        ret._db = open_lmdb(ret._write_db_filepath, ret._db_map_size, readonly)

        with ret._reader() as ro_txn:
            ret._length_length = int(ro_txn.get(_LENGTH_LENGTH_KEY))

        ret._max_length = 10 ** ret._length_length - 1
        ret._opened = True
        return ret

    def _close(self):

        self._opened = False
        self._db.close()

    @contextmanager
    def _tmp_close(self):

        if self._opened:
            self._db.close()

        try:
            yield

        finally:

            if self._opened:
                self._open(self._readonly)

    @contextmanager
    def _recursive_open(self, readonly):

        try:

            yield_ = self._open(readonly)
            need_close = True

        except RegisterAlreadyOpenError:

            yield_ = self
            need_close = False

        if not readonly and yield_._readonly:
            raise RegisterAlreadyOpenError(yield_)

        try:
            yield yield_

        finally:

            if need_close:

                yield_._close()
                yield_._opened = False

    def _check_open_raise(self, method_name):

        if not self._opened:
            raise RegisterNotOpenError(
                f"The `Register` \"{self._shorthand}\" is not open. In order to call the method `{method_name}`, "
                f"you must open the `Register` via `with {self._shorthand}.open() as {self._shorthand}:`."
            )

    def _check_not_open_raise(self, method_name):

        if self._opened:
            raise RegisterOpenError(
                f"The `Register` \"{self._shorthand}\" cannot be `open` when you call the method `{method_name}`."
            )

    def _check_readwrite_raise(self, method_name):
        """Call `self._check_open_raise` before this method."""

        if self._readonly:
            raise RegisterError(
                f"The `Register` \"{self._shorthand}\" is `open`ed in read-only mode. In order to call the method "
                f"`{method_name}`, you must `open` this `Register` in read-write mode via `with {self._shorthand}."
                f"open() as {self._shorthand}:`."
            )

    def _set_local_dir(self, local_dir):
        """`local_dir` and a corresponding register database must exist prior to calling this method.

        :param local_dir: (type `pathlib.Path`) Absolute.
        """

        if not local_dir.is_absolute():
            raise ValueError(NOT_ABSOLUTE_ERROR_MESSAGE.format(str(local_dir)))

        if local_dir.parent != self.dir:
            raise ValueError(
                f"The `local_dir` argument must be a sub-directory of `{self._shorthand}.dir`.\n" +
                f"`local_dir.parent` : {str(local_dir.parent)}\n"
                f"`{self._shorthand}.dir` : {str(self.dir)}"
            )

        check_reg_structure(local_dir)
        self._local_dir = local_dir
        self._local_dir_bytes = str(self._local_dir).encode("ASCII")
        self._perm_db_filepath = self._local_dir / DATABASE_FILEPATH
        self._write_db_filepath = self._perm_db_filepath
        self._subreg_bytes = _SUB_KEY_PREFIX + self._local_dir_bytes
        self._version_filepath = self._local_dir / VERSION_FILEPATH
        self._msg_filepath = self._local_dir / MSG_FILEPATH
        self._cls_filepath = self._local_dir / CLS_FILEPATH
        self._shorthand_filepath = self._local_dir / SHORTHAND_FILEPATH
        self._db_map_size_filepath = self._local_dir / MAP_SIZE_FILEPATH
        self._digest_filepath = self._local_dir / DIGEST_FILEPATH

    def _has_compatible_version(self):
        return self._version in COMPATIBLE_VERSIONS

    @contextmanager
    def _time(self, elapsed_name):

        start_time = time.time()

        try:
            yield

        finally:
            self.__dict__[elapsed_name] += time.time() - start_time

    #################################
    #      PROTEC INFO METHODS      #

    def _relational_encode_info(self, info, r_txn):

        encoder = _RelationalInfoJsonEncoder(
            self,
            r_txn,
            ensure_ascii = True,
            allow_nan = True,
            indent = None,
            separators = (',', ':')
        )
        return info.to_json(encoder).encode("ASCII")

    def _relational_decode_info(self, cls, json, r_txn):

        str_hook = _RelationalApriInfoStrHook(self, r_txn)
        return cls.from_json(json.decode("ASCII"), str_hook)

    #################################
    #      PUBLIC APRI METHODS      #

    def apris(self, sort = False, diskonly = False, recursively = False):

        self._check_open_raise("apris")
        check_type(diskonly, "diskonly", bool)
        check_type(recursively, "recursively", bool)

        if not diskonly:
            apris_ram_gen = self._apris_ram()

        else:
            apris_ram_gen = []

        with self._reader() as ro_txn:

            apris_disk_gen = map(lambda t: t[0], self._apris_disk(ro_txn))

            if recursively:
                apris_recursive_gen = self._apris_recursive(ro_txn)

            else:
                apris_recursive_gen = []

            apris = itertools.chain(apris_ram_gen, apris_disk_gen, apris_recursive_gen)

            if sort:
                yield from sorted(list(apris))

            else:
                yield from apris

    def change_apri(self, old_apri, new_apri, diskonly = False):
        """Replace an old `ApriInfo`, and all references to it, with a new `ApriInfo`.

        If ANY `Block`, `ApriInfo`, or `AposInfo` references `old_apri`, its entries in this `Register` will be
        updated to reflect the replacement of `old_apri` with `new_apri`. (See example below.) After the replacement
        `old_apri` -> `new_apri` is made, the set of `ApriInfo` that changed under that replacement must be disjoint
        from the set of `ApriInfo` that did not change. Otherwise, a `DataExistsError` is raised.

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
        :raise DataExistsError: See above.
        """

        self._check_open_raise("change_apri")
        self._check_readwrite_raise("change_apri")
        check_type(old_apri, "old_apri", ApriInfo)
        check_type(new_apri, "new_apri", ApriInfo)
        check_type(diskonly, "diskonly", bool)

        if old_apri == new_apri:
            return

        if not diskonly:
            self._change_apri_ram(old_apri, new_apri)

        with self._reader() as ro_txn:
            old_id, old_apri_id_key, old_id_apri_key = self._change_apri_pre(old_apri, None, True, new_apri, ro_txn)

        with self._writer() as rw_txn:
            self._change_apri_disk(old_id, old_apri_id_key, old_id_apri_key, new_apri, rw_txn)

    def rmv_apri(self, apri, force = False, missing_ok = False, diskonly = False):

        self._check_open_raise("rmv_apri")
        self._check_readwrite_raise("rmv_apri")
        check_type(apri, "apri", ApriInfo)
        check_type(force, "force", bool)
        check_type(missing_ok, "missing_ok", bool)

        missing = True

        if not diskonly:

            try:
                self._rmv_apri_ram(apri, force)

            except DataNotFoundError:
                pass

            else:
                missing = False

        with self._reader() as ro_txn:

            try:
                keys, blk_filenames, compressed_filenames = self._rmv_apri_pre(apri, None, True, force, ro_txn)

            except DataNotFoundError:
                pass

            else:
                missing = False

        if not missing_ok and missing:
            raise DataNotFoundError(_NO_APRI_ERROR_MESSAGE.format(apri, self))

        rrw_txn = None

        try:

            with self._reversible_writer() as rrw_txn:
                self._rmv_apri_disk(keys, rrw_txn)

            if force:
                type(self)._rmv_apri_disk2(blk_filenames, compressed_filenames)

        except BaseException as e:

            if rrw_txn is not None and force:

                with self._writer() as rw_txn:
                    ee = self._rmv_apri_error(blk_filenames, compressed_filenames, rw_txn, rrw_txn, e)

                raise ee

            else:
                raise

    def __contains__(self, apri):

        self._check_open_raise("__contains__")

        if self.___contains___ram(apri):
            return True

        with self._reader() as ro_txn:

            try:
                apri_id_key = self.___contains___pre(apri, None, True, ro_txn)

            except DataNotFoundError:
                return False

            else:
                return self.___contains___disk(apri_id_key, ro_txn)

    def num_apri(self):

        self._check_open_raise("num_apri")
        ram_apri = set(self._apris_ram())
        ret = len(ram_apri)

        with self._reader() as ro_txn:

            for apri in self._apris_disk(ro_txn):

                if apri not in ram_apri:
                    ret += 1

        return ret

    def __iter__(self):
        return iter(self.apris())

    #################################
    #      PROTEC APRI METHODS      #

    def _change_apri_ram(self, old_apri, new_apri):

        if self.___contains___ram(old_apri):
            raise DataNotFoundError(_NO_APRI_ERROR_MESSAGE.format(old_apri, self))

        if self.___contains___ram(new_apri):
            warnings.warn(f"This `Register` already has a reference to {new_apri}.")

        for apri in self._apris_ram():

            if old_apri in apri:

                apri_ = apri.change_info(old_apri, new_apri)

                for blk in self._blks_ram(apri):

                    try:
                        seg = blk.segment()

                    except BlockNotOpenError as e:
                        raise BlockNotOpenError(_RAM_BLOCK_NOT_OPEN_ERROR_MESSAGE.format(apri), blk.startn()) from e

                    blk_ = Block(seg, apri_, blk.startn())
                    self.add_ram_blk(blk_)

                del self._ram_blks[apri]

    def _change_apri_pre(self, old_apri, old_apri_json, old_reencode, new_apri, r_txn):

        if old_reencode:
            old_apri_json = self._relational_encode_info(old_apri, r_txn)

        try:
            new_apri_json = self._relational_encode_info(new_apri, r_txn)

        except DataNotFoundError:
            pass

        else:

            new_apri_id_key = self.___contains___pre(new_apri, new_apri_json, False, r_txn)

            if Register.___contains___disk(new_apri_id_key, r_txn):
                raise DataExistsError(f"This `Register` already has a reference to {new_apri}.")

        old_apri_id_key = Register._get_apri_id_key(old_apri_json)
        old_id = self._get_apri_id(old_apri, old_apri_json, False, r_txn)
        old_id_apri_key = Register._get_id_apri_key(old_id)
        return old_id, old_apri_id_key, old_id_apri_key

    def _change_apri_disk(self, old_id, old_apri_id_key, old_id_apri_key, new_apri, rw_txn):

        rw_txn.delete(old_apri_id_key)
        rw_txn.delete(old_id_apri_key)
        self._add_apri_disk(new_apri, [old_id], True, rw_txn)

        try:
            new_apri_json = self._relational_encode_info(new_apri, rw_txn)

        except DataNotFoundError as e:
            raise RegisterError from e # see pattern IV.4

        rw_txn.put(old_id_apri_key, new_apri_json)
        rw_txn.put(Register._get_apri_id_key(new_apri_json), old_id)

    def _apris_ram(self):
        yield from self._ram_blks.keys()

    def _apris_disk(self, r_txn):

        with r_txn_prefix_iter(_ID_APRI_KEY_PREFIX, r_txn) as it:

            for _, apri_json in it:
                yield self._relational_decode_info(ApriInfo, apri_json, r_txn), apri_json

    def _apris_recursive(self, r_txn):

        for subreg, ro_txn in self._subregs_bfs(True, r_txn):

            yield from subreg._apris_ram(False)
            yield from subreg._apris_disk(False, ro_txn)

    def ___contains___ram(self, apri):
        return apri in self._ram_blks.keys()

    def ___contains___pre(self, apri, apri_json, reencode, r_txn):

        if reencode:
            apri_json = self._relational_encode_info(apri, r_txn)

        return Register._get_apri_id_key(apri_json)

    @staticmethod
    def ___contains___disk(apri_id_key, r_txn):
        return r_txn_has_key(apri_id_key, r_txn)

    @staticmethod
    def _get_apri_json(apri_id, r_txn):
        """Get JSON bytestring representing an `ApriInfo` instance.

        :param apri_id: (type `bytes`)
        :param r_txn: (type `lmbd.Transaction`, default `None`) The transaction to query. If `None`, then use open a new
        transaction and commit it after this method resolves.
        :return: (type `bytes`)
        """

        apri_json = r_txn.get(Register._get_id_apri_key(apri_id), default = None)

        if apri_json is not None:
            return apri_json

        else:
            raise RegisterError(f"Missing `ApriInfo` id : {apri_id}")

    @staticmethod
    def _get_apri_id_key(apri_json):
        return _APRI_ID_KEY_PREFIX + apri_json

    @staticmethod
    def _get_id_apri_key(apri_id):
        return _ID_APRI_KEY_PREFIX + apri_id

    def _get_apri_id(self, apri, apri_json, reencode, r_txn):
        """Get an `ApriInfo` ID for this database. If `missing_ok is True`, then create an ID if the passed `apri` or
        `apri_json` is unknown to this `Register`.

        One of `apri` and `apri_json` can be `None`, but not both. If both are not `None`, then `apri` is used.

        `self._db` must be opened by the caller.

        :param apri: (type `ApriInfo`)
        :param apri_json: (type `bytes`)
        :param r_txn: (type `lmbd.Transaction`, default `None`) The transaction to query. If `None`, then open a new
        transaction and commit it after this method returns.
        :raises DataNotFoundError: If `apri` or `apri_json` is not known to this `Register`.
        :return: (type `bytes`)
        """

        if reencode:
            # uncaught `DataNotFoundError` (see pattern VI.1)
            apri_json = self._relational_encode_info(apri, r_txn)

        key = Register._get_apri_id_key(apri_json)
        apri_id = r_txn.get(key, default = None)

        if apri_id is None:
            # see pattern VI.1
            raise DataNotFoundError(_NO_APRI_ERROR_MESSAGE.format(apri, self))

        else:
            return apri_id

    @staticmethod
    def _get_new_id(reserved, rw_txn):

        for next_apri_id_num in range(int(rw_txn.get(_CURR_ID_KEY)), _MAX_NUM_APRI):

            next_id = bytify_int(next_apri_id_num, _MAX_NUM_APRI_LEN)

            if next_id not in reserved:
                break

        else:
            raise RegisterError(f"Too many apris added to this `Register`, the limit is {_MAX_NUM_APRI}.")

        rw_txn.put(_CURR_ID_KEY, bytify_int(next_apri_id_num + 1, _MAX_NUM_APRI_LEN))
        return next_id

    def _add_apri_ram(self, apri, exclude_root):

        for key, inner_info in apri.iter_inner_info():

            if (
                (not exclude_root or key is not None) and
                isinstance(inner_info, ApriInfo) and
                not self.___contains___ram(inner_info)
            ):
                self._ram_blks[inner_info] = []

    def _add_apri_disk(self, apri, reserved, exclude_root, rw_txn):

        for key, inner_info in apri.iter_inner_info("dfs"):

            if (not exclude_root or key is not None) and isinstance(inner_info, ApriInfo):

                try:
                    inner_apri_json = self._relational_encode_info(inner_info, rw_txn)

                except DataNotFoundError as e:
                    raise RegisterError from e # see pattern VI.4

                try:
                    apri_id_key = self.___contains___pre(inner_info, inner_apri_json, False, rw_txn)

                except DataNotFoundError as e:
                    raise RegisterError from e # see pattern VI.4

                if not self.___contains___disk(apri_id_key, rw_txn):

                    id_ = Register._get_new_id(reserved, rw_txn)
                    Register._add_apri_disk_helper(inner_info, inner_apri_json, id_, rw_txn)

    @staticmethod
    def _add_apri_disk_helper(apri, apri_json, id_, rw_txn):

        apri_id_key = Register._get_apri_id_key(apri_json)
        id_apri_key = Register._get_id_apri_key(id_)
        rw_txn.put(apri_id_key, id_)
        rw_txn.put(id_apri_key, apri_json)

        if 8 + 6 + len(apri_id_key) > 4096:
            warnings.warn(f"Long `ApriInfo` result in disk memory inefficiency. Long `ApriInfo`: {apri}.")

    @staticmethod
    def _disk_apri_key_exists(apri_id_key, r_txn):

        if apri_id_key is None:
            return False

        else:
            return r_txn_has_key(apri_id_key, r_txn)

    def _rmv_apri_ram(self, apri, force):

        if self.___contains___ram(apri):

            for inner in apri.iter_inner_info():

                if isinstance(inner, ApriInfo):

                    if self._num_blks_ram(apri) == 0 or force:
                        del self._ram_blks[apri]

                    else:
                        raise RegisterError

        else:
            raise DataNotFoundError(_NO_APRI_ERROR_MESSAGE.format(apri, self))

    def _rmv_apri_pre(self, apri, apri_json, reencode, force, r_txn):

        if reencode:
            apri_json = self._relational_encode_info(apri, r_txn)

        if not self._disk_apri_key_exists(Register._get_apri_id_key(apri_json), r_txn):
            raise DataNotFoundError(_NO_APRI_ERROR_MESSAGE.format(apri, self))

        keys = []
        blk_filenames = []
        compressed_filenames = []

        for apri_, apri_json_ in self._apris_disk(r_txn):

            if apri in apri_:

                if not force and apri_ not in apri: # if not force and apri != apri_
                    raise DataExistsError(
                        "The following apri references the given apri (set `force = True` to remove both apri) :\n"
                        f"{apri_}\n{apri}\n{self}"
                    )

                keys.append(Register._get_apri_id_key(apri_json_))
                keys.append(Register._get_id_apri_key(self._get_apri_id(apri_, apri_json_, False, r_txn)))
                prefix = self._num_blks_pre(apri_, apri_json_, False, r_txn)

                if Register._num_blks_disk(prefix, r_txn) > 0:

                    if not force:
                        raise DataExistsError(
                            "The given apri has associated `Block`s (set `force = True` to remove the `Block`s as "
                            f"well) :\n{apri}\n{self}"
                        )

                    else:

                        blk_prefix, compressed_prefix = self._get_disk_blk_prefixes(apri_, apri_json_, False, r_txn)

                        with r_txn_prefix_iter(blk_prefix, r_txn) as blk_it:

                            with r_txn_prefix_iter(compressed_prefix, r_txn) as compressed_it:

                                for (blk_key, _), (compressed_key, _) in zip(blk_it, compressed_it):

                                    blk_filename, compressed_filename = self._get_disk_blk_filenames(
                                        blk_key, compressed_key, r_txn
                                    )
                                    keys.append(blk_key)
                                    blk_filenames.append(blk_filename)
                                    keys.append(compressed_key)
                                    compressed_filenames.append(compressed_filename)

                apos_key = self._get_apos_key(apri_, apri_json_, False, r_txn)

                if self._apos_key_exists(apos_key, r_txn):

                    if not force:
                        raise DataExistsError(
                            "The given apri has associated apos (set `force = True` to remove the apos as well) :\n"
                            f"{apri}\n{self}"
                        )

                    else:
                        keys.append(apos_key)

        return keys, blk_filenames, compressed_filenames

    @staticmethod
    def _rmv_apri_disk(keys, rw_txn):

        for key in keys:
            rw_txn.delete(key)

    @classmethod
    def _rmv_apri_disk2(cls, blk_filenames, compressed_filenames):

        if _debug == 1:
            raise KeyboardInterrupt

        for blk_filename, compressed_filename in zip(blk_filenames, compressed_filenames):

            if _debug == 2:
                raise KeyboardInterrupt

            cls._rmv_disk_blk_disk2(blk_filename, compressed_filename, {})

            if _debug == 3:
                raise KeyboardInterrupt

        if _debug == 4:
            raise KeyboardInterrupt

    def _rmv_apri_error(self, blk_filenames, compressed_filenames, rw_txn, rrw_txn, e):

        no_recover = RegisterRecoveryError(f"The following `Register` failed to recover from `rmv_apri` :\n{self}")
        no_recover.__cause__ = e

        if isinstance(e, RegisterRecoveryError):
            return e

        try:

            for blk_filename, compressed_filename in zip(blk_filenames, compressed_filenames):

                if compressed_filename is not None:

                    if compressed_filename.exists():

                        try:
                            blk_filename.touch()

                        except FileExistsError:
                            pass

                    else:
                        return no_recover

                elif not blk_filename.exists():
                    return no_recover

            rrw_txn.reverse(rw_txn)
            return e

        except BaseException as ee:

            no_recover.__cause__ = ee
            return no_recover

    #################################
    #      PUBLIC APOS METHODS      #

    def set_apos(self, apri, apos, exists_ok = False):

        self._check_open_raise("set_apos")
        self._check_readwrite_raise("set_apos")
        check_type(apri, "apri", ApriInfo)
        check_type(apos, "apos", AposInfo)

        with self._reader() as ro_txn:

            add_apri, add_apos_inner, apos_key, apos_json = self._set_apos_pre(
                apri, None, True, apos, None, True, exists_ok, ro_txn
            )

            if _debug == 2:
                time.sleep(10 ** 8)

        with self._writer() as rw_txn:

            self._set_apos_disk(apri, apos, add_apri, add_apos_inner, apos_key, apos_json, rw_txn)

            if _debug == 1:
                time.sleep(10 ** 8)

    def apos(self, apri, recursively = False):
        """Get some `AposInfo` associated with a given `ApriInfo`.

        :param apri: (type `ApriInfo`)
        :raises DataNotFoundError: If no `AposInfo` has been associated to `apri`.
        :return: (type `AposInfo`)
        """

        self._check_open_raise("apos")
        check_type(apri, "apri", ApriInfo)

        with self._reader() as ro_txn:

            try:
                apos_key = self._apos_pre(apri, None, True, ro_txn)

            except DataNotFoundError:

                if not recursively:
                    raise

            else:
                return self._apos_disk(apos_key, ro_txn)

            return self._apos_recursive(apri, ro_txn)

    def rmv_apos(self, apri, missing_ok = False):

        self._check_open_raise("rmv_apos")
        self._check_readwrite_raise("rmv_apos")
        check_type(apri, "apri", ApriInfo)
        check_type(missing_ok, "missing_ok", bool)

        with self._reader() as ro_txn:
            apos_key, missing = self._rmv_apos_pre(apri, None, True, missing_ok, ro_txn)

        if missing:
            return

        with self._writer() as rw_txn:
            Register._rmv_apos_disk(apos_key, rw_txn)

    #################################
    #      PROTEC APOS METHODS      #

    def _apos_pre(self, apri, apri_json, reencode, r_txn):

        if reencode:
            apri_json = self._relational_encode_info(apri, r_txn)

        apos_key = self._get_apos_key(apri, apri_json, False, r_txn)

        if not self._apos_key_exists(apos_key, r_txn):
            raise DataNotFoundError(_NO_APOS_ERROR_MESSAGE.format(apri, self))

        return apos_key

    def _apos_disk(self, apos_key, r_txn):
        return self._relational_decode_info(AposInfo, r_txn.get(apos_key), r_txn)

    def _apos_recursive(self, apri, r_txn):

        for subreg, ro_txn in self._subregs_bfs(True, r_txn):

            try:
                apos_key = self._apos_pre(apri, None, True, ro_txn)

            except DataNotFoundError:
                pass

            else:
                return subreg._apos_disk(apos_key, ro_txn)

        raise DataNotFoundError(_NO_APOS_ERROR_MESSAGE.format(apri, self))

    def _set_apos_pre(self, apri, apri_json, apri_reencode, apos, apos_json, apos_reencode, exists_ok, r_txn):

        try:

            if apri_reencode:
                apri_json = self._relational_encode_info(apri, r_txn)

        except DataNotFoundError:

            add_apri = True
            apos_key = None

        else:

            apos_key = self._get_apos_key(apri, apri_json, False, r_txn)
            apri_id_key = Register._get_apri_id_key(apri_json)
            add_apri = not Register._disk_apri_key_exists(apri_id_key, r_txn)

        try:

            if apos_reencode:
                apos_json = self._relational_encode_info(apos, r_txn)

        except DataNotFoundError:

            apos_json = None
            add_apos_inner = True

        else:
            add_apos_inner = False

        if not add_apri:

            if not exists_ok and Register._apos_key_exists(apos_key, r_txn):
                raise DataExistsError(
                    "An `AposInfo` is already associated with the following `ApriInfo` (please set `exists_ok = "
                    f"True` in order to overwrite the current `AposInfo`) : {apri}"
                )

            return add_apri, add_apos_inner, apos_key, apos_json

        else:
            return add_apri, add_apos_inner, apos_key, apos_json

    def _set_apos_disk(self, apri, apos, add_apri, add_apos_inner, apos_key, apos_json, rw_txn):

        if add_apri:

            self._add_apri_disk(apri, [], False, rw_txn)

            try:
                apri_json = self._relational_encode_info(apri, rw_txn)

            except DataNotFoundError as e:
                raise RegisterError from e #see pattern VI.4

            else:
                apos_key = self._get_apos_key(apri, apri_json, False, rw_txn)

        if add_apos_inner:

            self._add_apri_disk(apos, [], False, rw_txn)

            try:
                apos_json = self._relational_encode_info(apos, rw_txn)

            except DataNotFoundError as e:
                raise RegisterError from e  # see pattern VI.4

        rw_txn.put(apos_key, apos_json)

        if 6 + 8 + _APOS_KEY_PREFIX_LEN +  len(apos_json) > 4096:
            warnings.warn(f"Long `AposInfo` result in disk memory inefficiency. Long `AposInfo`: {str(apos)}.")

    def _rmv_apos_pre(self, apri, apri_json, reencode, missing_ok, r_txn):

        if reencode:
            apri_json = self._relational_encode_info(apri, r_txn)

        apos_key = self._get_apos_key(apri, apri_json, False, r_txn)
        missing = not Register._apos_key_exists(apos_key, r_txn)

        if not missing_ok and missing:
            raise DataNotFoundError(_NO_APOS_ERROR_MESSAGE.format(apri, self))

        return apos_key, missing

    @staticmethod
    def _rmv_apos_disk(apos_key, rw_txn):
        rw_txn.delete(apos_key)

    @staticmethod
    def _apos_key_exists(apos_key, r_txn):

        if apos_key is None:
            return False

        else:
            return r_txn_has_key(apos_key, r_txn)

    def _get_apos_key(self, apri, apri_json, reencode, r_txn):
        """Get a key for an `AposInfo` entry.

        One of `apri` and `apri_json` can be `None`, but not both. If both are not `None`, then `apri` is used.

        :param apri: (type `ApriInfo`)
        :param apri_json: (type `bytes`)
        :param r_txn: (type `lmbd.Transaction`) The transaction to query.
        :raises DataNotFoundError: If `apri` is not known to this `Register`.
        :return: (type `bytes`)
        """

        try:
            apri_id = self._get_apri_id(apri, apri_json, reencode, r_txn)

        except DataNotFoundError:
            return None

        else:
            return _APOS_KEY_PREFIX + apri_id

    #################################
    #  PUBLIC SUB-REGISTER METHODS  #

    def add_subreg(self, subreg, exists_ok = False):

        self._check_open_raise("add_subreg")
        subreg._check_open_raise("add_subreg")
        self._check_readwrite_raise("add_subreg")
        check_type(subreg, "subreg", Register)
        check_type(exists_ok, "exists_ok", bool)

        with self._reader() as self_ro_txn:

            with subreg._reader() as subreg_ro_txn:

                subreg_key, exists = self._add_subreg_pre(subreg, exists_ok, self_ro_txn, subreg_ro_txn)

        if exists:
            return

        with self._writer() as rw_txn:
            Register._add_subreg_disk(subreg_key, rw_txn)

    def rmv_subreg(self, subreg, missing_ok = False):
        """
        :param subreg: (type `Register`)
        """

        self._check_open_raise("rmv_subreg")
        self._check_readwrite_raise("rmv_subreg")
        check_type(subreg, "Register", Register)
        check_type(missing_ok, "missing_ok", bool)

        with self._reader() as ro_txn:
            subreg_key, missing = self._rmv_subreg_pre(subreg, missing_ok, ro_txn)

        if missing:
            return

        with self._writer() as rw_txn:
            Register._rmv_subreg_disk(subreg_key, rw_txn)

    def subregs(self):

        self._check_open_raise("subregs")

        with self._reader() as ro_txn:
            yield from Register._subregs_disk(ro_txn)

    #################################
    #  PROTEC SUB-REGISTER METHODS  #

    def _add_subreg_pre(self, subreg, exists_ok, self_r_txn, subreg_r_txn):

        subreg_key = subreg._get_subreg_key()
        exists = r_txn_has_key(subreg_key, self_r_txn)

        if not exists_ok and exists:
            raise DataExistsError(
                f"The following `Register` has already been added as a subregister.\n"
                f"Intended superregister : {self}\n"
                f"Intended subregister   : {subreg}"
            )

        elif not exists:

            if not subreg._check_no_cycles_from(self, subreg_r_txn):
                raise RegisterError(
                    "Attempting to add sub-register will created a directed cycle in the subregister relation.\nIntended "
                    f"super-register : {self}\nIntended sub-register : {subreg}"
                )

        return subreg_key, exists

    @staticmethod
    def _add_subreg_disk(subreg_key, rw_txn):
        rw_txn.put(subreg_key, _SUB_VAL)

    def _rmv_subreg_pre(self, subreg, missing_ok, r_txn):

        key = subreg._get_subreg_key()
        missing = not r_txn_has_key(key, r_txn)

        if not missing_ok and missing:
            raise DataNotFoundError(
                f"No subregister relation found (set `missing_ok = True` to suppress this error) : \n{self}\n{subreg}"
            )

        return key, missing

    @staticmethod
    def _rmv_subreg_disk(subreg_key, rw_txn):
        rw_txn.delete(subreg_key)

    def _check_no_cycles_from(self, original, r_txn, touched = None):
        """Checks if adding `self` as a subregister to `original` would not create any directed cycles containing the
        arc `original` -> `self` in the subregister relation.

        Returns `False` if a directed cycle would be created and `True` otherwise. If `self` is already a subregister
        of `original`, then return `True` if the currently existing relation has no directed cycles that pass through
        `self`, and `False` otherwise. If `self == original`, then return `False`.

        :param original: (type `Register`)
        :param touched: used for recursion.
        :return: (type `bool`)
        """

        if self is original:
            return False

        if touched is None:
            touched = set()

        if any(original is subreg for subreg in Register._subregs_disk(r_txn)):
            return False

        for subreg in Register._subregs_disk(r_txn):

            if subreg not in touched:

                with subreg._recursive_open(True) as subreg:

                    with subreg._reader() as ro_txn:

                        touched.add(subreg)

                        if not subreg._check_no_cycles_from(original, ro_txn, touched):
                            return False

        else:
            return True

    @staticmethod
    def _subregs_disk(r_txn):

        with r_txn_prefix_iter(_SUB_KEY_PREFIX, r_txn) as it:

            for key, _ in it:

                local_dir = Path(key[_SUB_KEY_PREFIX_LEN : ].decode("ASCII"))
                subreg = Register._from_local_dir(local_dir)
                yield subreg

    def _get_subreg_key(self):
        return _SUB_KEY_PREFIX + self._local_dir_bytes

    def _subregs_bfs(self, exclude_root, r_txn):

        queue = [(self, r_txn)]
        touched = set()
        front_index = 0

        with ExitStack() as ro_txn_stack:

            while front_index < len(queue):

                if not exclude_root or front_index > 0:

                    reg, r_txn = queue[front_index]
                    front_index += 1
                    yield reg, r_txn

                elif exclude_root:
                    front_index += 1

                for subreg in Register._subregs_disk(r_txn):

                    if subreg not in touched:

                        touched.add(subreg)
                        ro_txn = ro_txn_stack.enter_context(subreg._reader())
                        queue.append((subreg, ro_txn))

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

        try:
            filename.unlink()

        except FileNotFoundError as e:
            raise DataNotFoundError from e

    def add_disk_blk(self, blk, exists_ok = False, dups_ok = True, ret_metadata = False, **kwargs):

        with self._time("add_elapsed"):

            self._check_open_raise("add_disk_blk")
            self._check_readwrite_raise("add_disk_blk")
            self._check_blk_open_raise(blk, "add_disk_blk")
            check_type(blk, "blk", Block)
            check_type(exists_ok, "exists_ok", bool)
            check_type(dups_ok, "dups_ok", bool)
            check_type(ret_metadata, "ret_metadata", bool)

            if len(blk) > self._max_length:
                raise ValueError

            startn_head = blk.startn() // self._startn_tail_mod

            if startn_head != self._startn_head:
                raise IndexError(
                    "The `startn` for the passed `Block` does not have the correct head:\n"
                    f"`tail_len`      : {self._startn_tail_length}\n"
                    f"expected `head` : {self._startn_head}\n"
                    f"`startn`        : {blk.startn()}\n"
                    f"`startn` head   : {startn_head}\n"
                    "Please see the method `set_startn_info` to troubleshoot this error."
                )

            with self._reader() as ro_txn:
                blk_key, compressed_key, filename, add_apri = self._add_disk_blk_pre(
                    blk.apri(), None, True, blk.startn(), len(blk), exists_ok, dups_ok, ro_txn
                )

            rrw_txn = None

            try:

                with self._reversible_writer() as rrw_txn:
                    self._add_disk_blk_disk(
                        blk.apri(), blk.startn(), len(blk), blk_key, compressed_key, filename, add_apri, rrw_txn
                    )

                return type(self)._add_disk_blk_disk2(blk.segment(), filename, ret_metadata, kwargs)

            except BaseException as e:

                if rrw_txn is not None:

                    with self._writer() as rw_txn:
                        ee = self._add_disk_blk_error(filename, rw_txn, rrw_txn, e)

                    raise ee

                else:
                    raise

    def append_disk_blk(self, blk, ret_metadata = False, **kwargs):

        with self._time("add_elapsed"):

            self._check_open_raise("append_disk_blk")
            self._check_readwrite_raise("append_disk_blk")
            self._check_blk_open_raise(blk, "append_disk_blk")
            check_type(blk, "blk", Block)
            check_type(ret_metadata, "ret_metadata", bool)

            if len(blk) > self._max_length:
                raise ValueError

            with self._reader() as ro_txn:
                blk_key, compressed_key, filename, add_apri, startn = self._append_disk_blk_pre(
                    blk.apri(), None, True, blk.startn(), len(blk), ro_txn
                )

            rrw_txn = None

            try:

                with self._reversible_writer() as rrw_txn:
                    self._add_disk_blk_disk(
                        blk.apri(), blk.startn(), len(blk), blk_key, compressed_key, filename, add_apri, rrw_txn
                    )

                file_metadata = type(self)._add_disk_blk_disk2(blk.segment(), filename, ret_metadata, kwargs)

                if ret_metadata:
                    return startn, file_metadata

                else:
                    return startn

            except BaseException as e:

                if rrw_txn is not None:

                    with self._writer() as rw_txn:
                        ee = self._add_disk_blk_error(filename, rw_txn, rrw_txn, e)

                    raise ee

                else:
                    raise

    def rmv_disk_blk(self, apri, startn = None, length = None, missing_ok = False, **kwargs):

        with self._time("rmv_elapsed"):

            self._check_open_raise("rmv_disk_blk")
            self._check_readwrite_raise("rmv_disk_blk")
            check_type(apri, "apri", ApriInfo)
            startn = check_return_int_None_default(startn, "startn", None)
            length = check_return_int_None_default(length, "length", None)
            check_type(missing_ok, "missing_ok", bool)

            if startn is not None and startn < 0:
                raise ValueError("`startn` must be non-negative.")

            if length is not None and length < 0:
                raise ValueError("`length` must be non-negative.")

            try:

                with self._reader() as ro_txn:
                    ret = self._rmv_disk_blk_pre(apri, None, True, startn, length, ro_txn)

            except DataNotFoundError:

                if not missing_ok:
                    raise

                else:
                    return

            startn_, length_, blk_key, compressed_key, blk_filename, compressed_filename = ret
            rrw_txn = None

            try:

                with self._reversible_writer() as rrw_txn:
                    Register._rmv_disk_blk_disk(blk_key, compressed_key, rrw_txn)

                self._rmv_disk_blk_disk2(blk_filename, compressed_filename, kwargs)

            except BaseException as e:

                if rrw_txn is not None:

                    with self._writer() as rw_txn:
                        ee = self._rmv_disk_blk_error(blk_filename, compressed_filename, rrw_txn, rw_txn, e)

                    raise ee

                else:
                    raise

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

        with self._reader() as ro_txn:

            try:
                blk_filename, compressed_filename = self._blk_metadata_pre(apri, None, True, startn, length, ro_txn)

            except DataNotFoundError:
                pass

            else:
                return Register._blk_metadata_disk(blk_filename, compressed_filename)

            if recursively:

                try:
                    return self._blk_metadata_recursive(apri, startn, length, ro_txn)

                except DataNotFoundError:
                    pass

            raise DataNotFoundError(self._blk_not_found_err_msg(
                False, True, recursively, apri, startn, length, None
            ))

    def compress(self, apri, startn = None, length = None, compression_level = 6, ret_metadata = False):

        with self._time("compress_elapsed"):

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

            with self._reader() as ro_txn:
                blk_key, compressed_key, blk_filename, compressed_filename = self._compress_pre(
                    apri, None, True, startn, length, ro_txn,
                )

            rrw_txn = None

            try:

                with self._reversible_writer() as rrw_txn:
                    Register._compress_disk(compressed_key, compressed_filename, rrw_txn)

                return type(self)._compress_disk2(blk_filename, compressed_filename, compression_level, ret_metadata)

            except BaseException as e:

                if rrw_txn is not None:

                    with self._writer() as rw_txn:
                        ee = self._compress_error(blk_filename, compressed_filename, rrw_txn, rw_txn, e)

                    raise ee

                else:
                    raise

    def decompress(self, apri, startn = None, length = None, ret_metadata = False):

        with self._time("decompress_elapsed"):

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

            with self._reader() as ro_txn:
                ret = self._decompress_pre(apri, None, True, startn, length, ro_txn)

            rrw_txn = None
            blk_key, compressed_key, blk_filename, compressed_filename, temp_blk_filename = ret

            try:

                with self._reversible_writer() as rrw_txn:
                    Register._decompress_disk(compressed_key, rrw_txn)

                return Register._decompress_disk2(blk_filename, compressed_filename, temp_blk_filename, ret_metadata)

            except BaseException as e:

                if rrw_txn is not None:

                    with self._writer() as rw_txn:
                        ee = Register._decompress_error(temp_blk_filename, rrw_txn, rw_txn, e)

                    raise ee

                else:
                    raise

    def is_compressed(self, apri, startn = None, length = None):

        self._check_open_raise("is_compressed")
        check_type(apri, "apri", ApriInfo)
        startn = check_return_int_None_default(startn, "startn", None)
        length = check_return_int_None_default(length, "length", None)

        if startn is not None and startn < 0:
            raise ValueError("`startn` must be non-negative.")

        if length is not None and length < 0:
            raise ValueError("`length` must be non-negative.")

        with self._reader() as ro_txn:

            compressed_key = self._is_compressed_pre(apri, None, True, startn, length, ro_txn)
            return Register._is_compressed_disk(compressed_key, ro_txn)

    #################################
    #    PROTEC DISK BLK METHODS    #

    def _get_disk_blk_prefixes(self, apri, apri_json, reencode, r_txn):

        blk_key, compressed_key = self._get_disk_blk_keys(apri, apri_json, reencode, 0, 1, r_txn)

        if blk_key is None:
            return None, None

        len1 = _BLK_KEY_PREFIX_LEN        + _MAX_NUM_APRI_LEN + _KEY_SEP_LEN
        len2 = _COMPRESSED_KEY_PREFIX_LEN + _MAX_NUM_APRI_LEN + _KEY_SEP_LEN
        return blk_key[ : len1], compressed_key[ : len2]

    def _get_disk_blk_prefixes_startn(self, apri, apri_json, reencode, startn, r_txn):

        blk_key, compressed_key = self._get_disk_blk_keys(apri, apri_json, reencode, startn, 1, r_txn)

        if blk_key is None:
            return None

        len1 = _BLK_KEY_PREFIX_LEN        + _MAX_NUM_APRI_LEN + _KEY_SEP_LEN + self._startn_tail_length + _KEY_SEP_LEN
        len2 = _COMPRESSED_KEY_PREFIX_LEN + _MAX_NUM_APRI_LEN + _KEY_SEP_LEN + self._startn_tail_length + _KEY_SEP_LEN
        return blk_key[: len1], compressed_key[: len2]

    def _add_disk_blk_pre(self, apri, apri_json, reencode, startn, length, exists_ok, dups_ok, r_txn):

        try:

            if reencode:
                apri_json = self._relational_encode_info(apri, r_txn)

        except DataNotFoundError:
            add_apri = True

        else:

            apri_id_key = Register._get_apri_id_key(apri_json)
            add_apri = not Register._disk_apri_key_exists(apri_id_key, r_txn)

        filename = random_unique_filename(self._local_dir, suffix = type(self).file_suffix, length = 6)

        if not add_apri:

            blk_key, compressed_key = self._get_disk_blk_keys(apri, apri_json, False, startn, length, r_txn)

            if not exists_ok and r_txn_has_key(blk_key, r_txn):
                raise DataExistsError(
                    f"Duplicate `Block` with the following data already exists in this `Register`: {apri}, startn = "
                    f"{startn}, length = {length}."
                )

            if not dups_ok:

                prefix = self._intervals_pre(apri, apri_json, False, r_txn)
                int1 = (startn, length)

                for int2 in self._intervals_disk(prefix, r_txn):

                    if intervals_overlap(int1, int2):
                        raise DataExistsError(
                            "Attempted to add a `Block` with duplicate indices. Set `dups_ok` to `True` to suppress."
                        )

        else:
            blk_key = compressed_key = None

        if length == 0:
            warnings.warn(f"Added a length 0 disk `Block` to {self}.\n{apri}, startn = {startn}")

        return blk_key, compressed_key, filename, add_apri

    def _add_disk_blk_disk(self, apri, startn, length, blk_key, compressed_key, filename, add_apri, rw_txn):

        if add_apri:

            self._add_apri_disk(apri, [], False, rw_txn)
            blk_key, compressed_key = self._get_disk_blk_keys(apri, None, True, startn, length, rw_txn)

        filename_bytes = filename.name.encode("ASCII")
        rw_txn.put(blk_key, filename_bytes)
        rw_txn.put(compressed_key, _IS_NOT_COMPRESSED_VAL)

    @classmethod
    def _add_disk_blk_disk2(cls, seg, filename, ret_metadata, kwargs):

        if _debug == 1:
            raise KeyboardInterrupt

        cls.dump_disk_data(seg, filename, **kwargs)

        if _debug == 2:
            raise KeyboardInterrupt

        if ret_metadata:
            return FileMetadata.from_path(filename)

        else:
            return None

    def _add_disk_blk_error(self, filename, rw_txn, rrw_txn, e):

        if isinstance(e, RegisterRecoveryError):
            return e

        try:

            if filename is not None:

                try:
                    filename.unlink()

                except FileNotFoundError:
                    pass

            rrw_txn.reverse(rw_txn)

        except BaseException as ee:

            eee = RegisterRecoveryError("Could not successfully recover from a failed disk `Block` add!")
            eee.__cause__ = ee
            return eee

        else:

            if isinstance(e, lmdb.MapFullError):

                ee = RegisterError(_MEMORY_FULL_ERROR_MESSAGE.format(self._db_map_size))
                ee.__cause__ = e
                return ee

            else:
                return e

    def _append_disk_blk_pre(self, apri, apri_json, reencode, startn, length, r_txn):

        try:

            if reencode:
                apri_json = self._relational_encode_info(apri, r_txn)

        except DataNotFoundError:
            add_apri = True

        else:

            apri_id_key = Register._get_apri_id_key(apri_json)
            add_apri = not Register._disk_apri_key_exists(apri_id_key, r_txn)

        filename = random_unique_filename(self._local_dir, suffix = type(self).file_suffix, length = 6)

        if not add_apri:

            try:
                prefix = self._maxn_pre(apri, apri_json, False, r_txn)

            except DataNotFoundError: # if apri has no disk blks (used passed startn)
                pass

            else:
                startn = self._maxn_disk(prefix, r_txn) + 1

            blk_key, compressed_key = self._get_disk_blk_keys(apri, apri_json, False, startn, length, r_txn)

        else:
            blk_key = compressed_key = None

        return blk_key, compressed_key, filename, add_apri, startn

    def _check_blk_open_raise(self, blk, method_name):

        if blk._num_entered == 0:
            raise BlockNotOpenError(f"You must do `with blk:` before you call `{self._shorthand}.{method_name}()`.")

    def _blk_metadata_pre(self, apri, apri_json, reencode, startn, length, r_txn):

        errmsg = self._blk_not_found_err_msg(False, True, False, apri, startn, length, None)

        if reencode:
            apri_json = self._relational_encode_info(apri, r_txn)

        if not Register._disk_apri_key_exists(Register._get_apri_id_key(apri_json), r_txn):
            raise DataNotFoundError(errmsg)

        startn_, length_ = self._resolve_startn_length_disk(apri, apri_json, False, startn, length, r_txn)

        if startn_ is None:
            raise DataNotFoundError(errmsg)

        blk_key, compressed_key = self._get_disk_blk_keys(apri, apri_json, False, startn_, length_, r_txn)
        blk_filename, compressed_filename = self._get_disk_blk_filenames(blk_key, compressed_key, r_txn)

        return blk_filename, compressed_filename

    @staticmethod
    def _blk_metadata_disk(blk_filename, compressed_filename):

        if compressed_filename is None:
            return FileMetadata.from_path(blk_filename)

        else:
            return FileMetadata.from_path(compressed_filename)

    def _blk_metadata_recursive(self, apri, startn, length, r_txn):

        for subreg, ro_txn in self._subregs_bfs(True, r_txn):

            try:
                blk_filename, compressed_filename = subreg._blk_metadata_pre(apri, None, True, startn, length, r_txn)

            except DataNotFoundError:
                pass

            else:
                return Register._blk_metadata_disk(blk_filename, compressed_filename)

        raise DataNotFoundError(self._blk_not_found_err_msg(False, True, True, apri, startn, length, None))

    def _rmv_disk_blk_pre(self, apri, apri_json, reencode, startn, length, r_txn):

        errmsg = self._blk_not_found_err_msg(False, True, False, apri, startn, length, None)

        if reencode:
            apri_json = self._relational_encode_info(apri, r_txn) # raises `DataNotFoundError` (see pattern VI.1)

        startn_, length_ = self._resolve_startn_length_disk(apri, apri_json, False, startn, length, r_txn)

        if startn_ is None:
            raise DataNotFoundError(errmsg) # see pattern VI.1

        blk_key, compressed_key = self._get_disk_blk_keys(apri, apri_json, False, startn_, length_, r_txn)

        if not Register._disk_blk_keys_exist(blk_key, compressed_key, r_txn):
            raise DataNotFoundError(errmsg)

        blk_filename, compressed_filename = self._get_disk_blk_filenames(blk_key, compressed_key, r_txn)

        if not is_deletable(blk_filename):
            raise OSError(f"Cannot delete `Block` file `{str(blk_filename)}`.")

        if compressed_filename is not None and not is_deletable(compressed_filename):
            raise OSError(f"Cannot delete compressed `Block` file `{str(compressed_filename)}`.")

        return startn_, length_, blk_key, compressed_key, blk_filename, compressed_filename

    @staticmethod
    def _rmv_disk_blk_disk(blk_key, compressed_key, rw_txn):

        rw_txn.delete(blk_key)
        rw_txn.delete(compressed_key)

    @classmethod
    def _rmv_disk_blk_disk2(cls, blk_filename, compressed_filename, kwargs):

        if _debug == 1:
            raise KeyboardInterrupt

        if compressed_filename is not None:

            if _debug == 2:
                raise KeyboardInterrupt

            blk_filename.unlink()

            if _debug == 3:
                raise KeyboardInterrupt

            compressed_filename.unlink()

            if _debug == 4:
                raise KeyboardInterrupt

        else:

            if _debug == 5:
                raise KeyboardInterrupt

            cls.clean_disk_data(blk_filename, **kwargs)

            if _debug == 6:
                raise KeyboardInterrupt

        if _debug == 7:
            raise KeyboardInterrupt

    def _rmv_disk_blk_error(self, blk_filename, compressed_filename, rrw_txn, rw_txn, e):

        if isinstance(e, RegisterRecoveryError):
            return e

        sorta_no_recover = RegisterRecoveryError(
            "Encountered an error after cleaning data files and deleting LMDB keys. Despite this error, the following "
            f"`Register` is in a state as if `rmv_disk_blk` did not encounter an error :\n{self}"
        )
        sorta_no_recover.__cause__ = e

        try:

            if compressed_filename is not None:

                if compressed_filename.exists():

                    try:
                        blk_filename.touch()

                    except FileExistsError:
                        pass

                    rrw_txn.reverse(rw_txn)
                    return e

                else:
                    return sorta_no_recover

            elif not blk_filename.exists():
                return sorta_no_recover

            else:

                rrw_txn.reverse(rw_txn)
                return e

        except BaseException as ee:

            eee = RegisterRecoveryError(f"The following `Register` failed to recover from `rmv_disk_blk` :\n{self}")
            eee.__cause__ = ee
            return eee

    def _is_compressed_pre(self, apri, apri_json, reencode, startn, length, r_txn):

        errmsg = self._blk_not_found_err_msg(False, True, False, apri, startn, length, None)

        if reencode:
            apri_json = self._relational_encode_info(apri, r_txn) # raises `DataNotFoundError` (see pattern VI.1)

        startn_, length_ = self._resolve_startn_length_disk(apri, apri_json, False, startn, length, r_txn)

        if startn_ is None:
            raise DataNotFoundError(errmsg)

        blk_key, compressed_key = self._get_disk_blk_keys(apri, apri_json, False, startn_, length_, r_txn)

        if not Register._disk_blk_keys_exist(blk_key, compressed_key, r_txn):
            raise DataNotFoundError(errmsg)

        return compressed_key

    @staticmethod
    def _is_compressed_disk(compressed_key, r_txn):
        return r_txn.get(compressed_key) != _IS_NOT_COMPRESSED_VAL

    def _decompress_pre(self, apri, apri_json, reencode, startn, length, r_txn):

        errmsg = self._blk_not_found_err_msg(False, True, False, apri, startn, length, None)

        if reencode:
            apri_json = self._relational_encode_info(apri, r_txn) # raises `DataNotFoundError` (see pattern VI.1)

        startn_, length_ = self._resolve_startn_length_disk(apri, apri_json, False, startn, length, r_txn)

        if startn_ is None:
            raise DataNotFoundError(errmsg)

        blk_key, compressed_key = self._get_disk_blk_keys(apri, apri_json, False, startn_, length_, r_txn)

        if not Register._disk_blk_keys_exist(blk_key, compressed_key, r_txn):
            raise DataNotFoundError(errmsg)

        compressed_val = r_txn.get(compressed_key)

        if compressed_val == _IS_NOT_COMPRESSED_VAL:
            raise DecompressionError(
                "The disk `Block` with the following data is not compressed: " +
                f"{str(apri)}, startn = {startn_}, length = {length_}"
            )

        blk_filename = self._local_dir / r_txn.get(blk_key).decode("ASCII")
        compressed_filename = self._local_dir / compressed_val.decode("ASCII")
        temp_blk_filename = blk_filename.parent / (blk_filename.stem + "_temp")

        if not is_deletable(blk_filename):
            raise OSError(f"Cannot delete ghost file `{str(blk_filename)}`.")

        if not is_deletable(compressed_filename):
            raise OSError(f"Cannot delete compressed file `{str(compressed_filename)}`.")

        return blk_key, compressed_key, blk_filename, compressed_filename, temp_blk_filename

    @staticmethod
    def _decompress_disk(compressed_key, rrw_txn):
        rrw_txn.put(compressed_key, _IS_NOT_COMPRESSED_VAL)

    @staticmethod
    def _decompress_disk2(blk_filename, compressed_filename, temp_blk_filename, ret_metadata):

        if _debug == 1:
            raise KeyboardInterrupt

        temp_blk_filename.mkdir()

        with zipfile.ZipFile(compressed_filename, "r") as compressed_fh:

            if _debug == 2:
                raise KeyboardInterrupt

            compressed_fh.extract(blk_filename.name, temp_blk_filename)

            if _debug == 3:
                raise KeyboardInterrupt

        try:

            if _debug == 4:
                raise KeyboardInterrupt

            blk_filename.unlink()

            if _debug == 5:
                raise KeyboardInterrupt

            (temp_blk_filename / blk_filename.name).rename(blk_filename)

            if _debug == 6:
                raise KeyboardInterrupt

            compressed_filename.unlink()

            if _debug == 7:
                raise KeyboardInterrupt

            temp_blk_filename.rmdir()

            if _debug == 8:
                raise KeyboardInterrupt

        except BaseException as e:
            raise RegisterRecoveryError from e

        if ret_metadata:
            return FileMetadata.from_path(blk_filename)

    @staticmethod
    def _decompress_error(temp_blk_filename, rrw_txn, rw_txn, e):

        if isinstance(e, RegisterRecoveryError):
            return e

        try:

            try:
                shutil.rmtree(temp_blk_filename)

            except FileNotFoundError:
                pass

            rrw_txn.reverse(rw_txn)

        except BaseException as ee:

            eee = RegisterRecoveryError()
            eee.__cause__ = ee
            return ee

        else:
            return e

    def _compress_pre(self, apri, apri_json, reencode, startn, length, r_txn):

        errmsg = self._blk_not_found_err_msg(False, True, False, apri, startn, length, None)

        if reencode:
            apri_json = self._relational_encode_info(apri, r_txn) # raises `DataNotFoundError` (see pattern VI.1)

        startn_, length_ = self._resolve_startn_length_disk(apri, apri_json, False, startn, length, r_txn)

        if startn_ is None:
            raise DataNotFoundError(errmsg) # see pattern VI.1

        blk_key, compressed_key = self._get_disk_blk_keys(apri, apri_json, False, startn_, length_, r_txn)

        if not self._disk_blk_keys_exist(blk_key, compressed_key, r_txn):

            print(blk_key, compressed_key)
            raise DataNotFoundError(errmsg)

        compressed_val = r_txn.get(compressed_key)

        if compressed_val != _IS_NOT_COMPRESSED_VAL:
            raise CompressionError(
                "The disk `Block` with the following data has already been compressed: " +
                f"{str(apri)}, startn = {startn_}, length = {length_}"
            )

        blk_filename = self._local_dir / r_txn.get(blk_key).decode("ASCII")
        compressed_filename = random_unique_filename(self._local_dir, suffix = COMPRESSED_FILE_SUFFIX)

        return blk_key, compressed_key, blk_filename, compressed_filename

    @staticmethod
    def _compress_disk(compressed_key, compressed_filename, rrw_txn):

        compressed_val = compressed_filename.name.encode("ASCII")
        rrw_txn.put(compressed_key, compressed_val)

    @classmethod
    def _compress_disk2(cls, blk_filename, compressed_filename, compression_level, ret_metadata):

        if _debug == 1:
            raise KeyboardInterrupt

        with zipfile.ZipFile(
                compressed_filename,  # target filename
                "x",  # zip mode (write, but don't overwrite)
                zipfile.ZIP_DEFLATED,  # compression mode
                True,  # use zip64
                compression_level
        ) as compressed_fh:

            if _debug == 2:
                raise KeyboardInterrupt

            compressed_fh.write(blk_filename, blk_filename.name)

            if _debug == 3:
                raise KeyboardInterrupt

        if _debug == 4:
            raise KeyboardInterrupt

        try:

            if _debug == 5:
                raise KeyboardInterrupt

            cls.clean_disk_data(blk_filename)

            if _debug == 6:
                raise KeyboardInterrupt

            blk_filename.touch()

            if _debug == 7:
                raise KeyboardInterrupt

            if ret_metadata:
                return FileMetadata.from_path(compressed_filename)

            else:
                return None

        except BaseException as e:
            raise RegisterRecoveryError from e

    def _compress_error(self, blk_filename, compressed_filename, rrw_txn, rw_txn, e):

        if isinstance(e, RegisterRecoveryError):
            return e

        if not blk_filename.exists():

            ee = RegisterRecoveryError(
                f"Deleted `Block` data file. The `Register` `{self._shorthand}` could not recover from failed "
                "`compress`."
            )
            ee.__cause__ = e
            return ee

        try:

            rrw_txn.reverse(rw_txn)

            try:
                compressed_filename.unlink()

            except FileNotFoundError:
                pass

        except BaseException as ee:

            eee = RegisterRecoveryError()
            eee.__cause__ = ee
            return eee

        else:
            return e

    @staticmethod
    def _disk_blk_keys_exist(blk_key, compressed_key, r_txn):

        if blk_key is None:
            return False

        has_blk_key = r_txn_has_key(blk_key, r_txn)
        has_compressed_key = r_txn_has_key(compressed_key, r_txn)

        if has_blk_key == has_compressed_key:
            return has_blk_key

        else:
            raise RegisterError("Uncompressed/compressed `Block` key mismatch.")

    def _get_disk_blk_keys(self, apri, apri_json, reencode, startn, length, r_txn):
        """Get the database key for a disk `Block`.

        One of `info` and `apri_json` can be `None`, but not both. If both are not `None`, then `info` is used.
        `self._db` must be opened by the caller. This method only queries the database to obtain the `info` ID.

        :param apri: (type `ApriInfo`)
        :param apri_json: (types `bytes`)
        :param startn: (type `int`) The start index of the `Block`.
        :param length: (type `int`) The length_ of the `Block`.
        :param r_txn: (type `lmbd.Transaction`) The transaction to query.
        :raises DataNotFoundError: If `info` is not known to this `Register`.
        :return: (type `bytes`)
        """

        try:
            apri_id = self._get_apri_id(apri, apri_json, reencode, r_txn)

        except DataNotFoundError: # see pattern VI.1
            return None, None

        else:

            tail = bytify_int(startn % self._startn_tail_mod, self._startn_tail_length)
            op_length = bytify_int(self._max_length - length, self._length_length)
            suffix = apri_id + _KEY_SEP + tail + _KEY_SEP + op_length
            return _BLK_KEY_PREFIX + suffix, _COMPRESSED_KEY_PREFIX + suffix

    def _num_disk_blks(self, apri, apri_json, reencode, r_txn):

        try:

            if reencode:
                apri_json = self._relational_encode_info(apri, r_txn)

        except DataNotFoundError:
            return 0

        else:

            prefix, _ = self._get_disk_blk_prefixes(apri, apri_json, False, r_txn)
            return r_txn_count_keys(prefix, r_txn)

    def _iter_disk_blk_pairs(self, prefix, apri, apri_json, reencode, r_txn):
        """Iterate over key-value pairs for block entries.

        :param prefix: (type `bytes`)
        :param apri: (type `ApriInfo`)
        :param apri_json: (type `bytes`)
        :param r_txn: (type `lmbd.Transaction`)
        :raise DataNotFoundError: If `apri` is not a disk `ApriInfo`.
        :return: (type `bytes`) key
        :return: (type `bytes`) value
        """

        try:
            prefix += self._get_apri_id(apri, apri_json, reencode, r_txn) + _KEY_SEP

        except DataNotFoundError: # see pattern VI.1
            pass # see pattern VI.3

        else:

            with r_txn_prefix_iter(prefix, r_txn) as it:
                yield from it

    def _get_raw_startn_length(self, prefix_len, key):

        stop1 = prefix_len + _MAX_NUM_APRI_LEN
        stop2 = stop1 + _KEY_SEP_LEN + self._startn_tail_length

        return (
            key[prefix_len           : stop1], # apri id
            key[stop1 + _KEY_SEP_LEN : stop2], # startn
            key[stop2 + _KEY_SEP_LEN : ] # op_length
        )

    @staticmethod
    def _join_disk_blk_data(prefix, apri_id, startn_bytes, len_bytes):
        return (
                prefix +
                apri_id + _KEY_SEP +
                startn_bytes + _KEY_SEP +
                len_bytes
        )

    def _get_startn_length(self, prefix_len, key):
        """
        :param prefix_len: (type `int`) Positive.
        :param key: (type `bytes`)
        :return: (type `ApriInfo`)
        :return (type `int`) startn
        :return (type `int`) length, non-negative
        """

        _, startn_bytes, op_length_bytes = self._get_raw_startn_length(prefix_len, key)

        return (
            intify_bytes(startn_bytes) + self._startn_head * self._startn_tail_mod,
            self._max_length - intify_bytes(op_length_bytes)
        )

    def _get_disk_blk_filenames(self, blk_key, compressed_key, r_txn):

        blk_val = r_txn.get(blk_key)
        compressed_val = r_txn.get(compressed_key)
        blk_filename = self._local_dir / blk_val.decode("ASCII")

        if compressed_val != _IS_NOT_COMPRESSED_VAL:

            compressed_filename = self._local_dir / compressed_val.decode("ASCII")

            if not compressed_filename.exists() or not blk_filename.exists():
                raise RegisterError("Compressed `Block` file or ghost file seems to be missing!")

            return blk_filename, compressed_filename

        else:

            if not blk_filename.exists():
                raise RegisterError("`Block` file seems to be missing!")

            return blk_filename, None

    #################################
    #    PUBLIC RAM BLK METHODS     #

    def add_ram_blk(self, blk):

        self._check_open_raise("add_ram_blk")
        self._check_readwrite_raise("add_ram_blk")
        self._check_blk_open_raise(blk, "add_ram_blk")
        check_type(blk, "blk", Block)
        apri = blk.apri()

        if not self.___contains___ram(apri):
            self._add_apri_ram(blk.apri(), False)

        if self._num_blks_ram(apri) == 0:
            self._ram_blks[blk.apri()].append(blk)

        else:

            for i, blk_ in enumerate(self._blks_ram(apri)):

                try:
                    blk_len = len(blk_)

                except BlockNotOpenError as e:
                    raise BlockNotOpenError(
                        _RAM_BLOCK_NOT_OPEN_ERROR_MESSAGE.format(apri, blk_.startn())
                    ) from e

                if blk_ is blk:
                    break

                elif blk.startn() < blk_.startn() or (blk.startn() == blk_.startn() and len(blk) > blk_len):

                    self._ram_blks[apri].insert(i, blk)
                    break

            else:
                self._ram_blks[blk.apri()].append(blk)

    def rmv_ram_blk(self, blk):

        self._check_open_raise("add_ram_blk")
        check_type(blk, "blk", Block)
        apri = blk.apri()

        try:
            self._check_blk_open_raise(blk, "rmv_ram_blk")

        except BlockNotOpenError as e:
            raise BlockNotOpenError(_RAM_BLOCK_NOT_OPEN_ERROR_MESSAGE.format(apri, blk.startn())) from e

        errmsg = self._blk_not_found_err_msg(True, False, False, apri, blk.startn(), len(blk), None)

        if not self.___contains___ram(apri):
            raise DataNotFoundError(errmsg)

        for i, blk_ in enumerate(self._blks_ram(apri)):

            if blk_ is blk:

                del self._ram_blks[apri][i]
                return

        else:
            raise DataNotFoundError(errmsg)

    def rmv_all_ram_blks(self):

        self._check_open_raise("rmv_all_ram_blks")

        for apri in self._apris_ram():
            self._ram_blks[apri] = []

    #################################
    #    PROTEC RAM BLK METHODS     #

    #################################
    # PUBLIC RAM & DISK BLK METHODS #

    @contextmanager
    def blk_by_n(self, apri, n, diskonly = False, recursively = False, ret_metadata = False, **kwargs):

        with ExitStack() as post_yield: # see pattern VIII.1

            try:

                with ExitStack() as prior_yield:

                    prior_yield.enter_context(self._time("load_elapsed"))
                    self._check_open_raise("blk_by_n")
                    check_type(apri, "apri", ApriInfo)
                    n = check_return_int(n, "n")
                    check_type(diskonly, "diskonly", bool)
                    check_type(recursively, "recursively", bool)
                    check_type(ret_metadata, "ret_metadata", bool)

                    if n < 0:
                        raise IndexError("`n` must be non-negative.")

                    if not diskonly:

                        try:
                            yield_ = self._blk_by_n_ram(apri, n)

                        except DataNotFoundError:
                            pass

                        else:

                            if ret_metadata:
                                yield_ = (yield_, None)

                            raise FinalYield

                    ro_txn = prior_yield.enter_context(self._reader())

                    try:
                        blk_filename, startn = self._blk_by_n_pre(apri, None, True, n, ro_txn)

                    except DataNotFoundError:
                        pass

                    else:

                        yield_ = type(self)._blk_disk(blk_filename, apri, startn, ret_metadata, kwargs)
                        raise FinalYield

                    if recursively:

                        try:
                            yield_ = self._blk_by_n_recursive(apri, n, diskonly, ret_metadata, kwargs, ro_txn)

                        except DataNotFoundError:
                            pass

                        else:
                            raise FinalYield

                    raise DataNotFoundError(self._blk_not_found_err_msg(
                        not diskonly, True, recursively, apri, None, None, n
                    ))

            except FinalYield:

                if ret_metadata:
                    yield post_yield.enter_context(yield_[0]), yield_[1]

                else:
                    yield post_yield.enter_context(yield_)

    @contextmanager
    def blk(
        self, apri, startn = None, length = None, diskonly = False, recursively = False, ret_metadata = False, **kwargs
    ):

        with ExitStack() as post_yield: # see pattern VIII.1

            try:

                with ExitStack() as pre_yield:

                    pre_yield.enter_context(self._time("load_elapsed"))
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

                    if not diskonly:

                        try:
                            yield_ = self._blk_ram(apri, startn, length, ret_metadata)

                        except DataNotFoundError:
                            pass

                        else:
                            raise FinalYield

                    ro_txn = pre_yield.enter_context(self._reader())

                    try:
                        blk_filename, startn_ = self._blk_pre(apri, None, True, startn, length, ro_txn)

                    except DataNotFoundError:
                        pass

                    else:

                        yield_ = type(self)._blk_disk(blk_filename, apri, startn_, ret_metadata, kwargs)
                        raise FinalYield

                    if recursively:

                        try:
                            yield_ = self._blk_recursive(apri, startn, length, diskonly, ret_metadata, kwargs, ro_txn)

                        except DataNotFoundError:
                            pass

                        else:
                            raise FinalYield

                    raise DataNotFoundError(self._blk_not_found_err_msg(
                        not diskonly, True, recursively, apri, startn, length, None
                    ))

            except FinalYield:

                if ret_metadata:
                    yield post_yield.enter_context(yield_[0]), yield_[1]

                else:
                    yield post_yield.enter_context(yield_)

    def blks(self, apri, diskonly = False, recursively = False, ret_metadata = False, **kwargs):

        self._check_open_raise("blks")
        check_type(apri, "apri", ApriInfo)
        check_type(diskonly, "diskonly", bool)
        check_type(recursively, "recursively", bool)
        check_type(ret_metadata, "ret_metadata", bool)

        for blk in self._blks_ram(apri):

            with blk:
                yield blk

        with self._reader() as ro_txn:

            try:
                apri_json = self._relational_encode_info(apri, ro_txn)

            except DataNotFoundError:
                blks_disk_gen = []

            else:

                prefix = self._intervals_pre(apri, apri_json, False, ro_txn)
                blks_disk_gen = self._blks_disk(prefix, apri, apri_json, False, ret_metadata, kwargs, ro_txn)

            if recursively:
                blks_recursive_gen = self._blks_recursive(apri, diskonly, ret_metadata, kwargs, ro_txn)

            else:
                blks_recursive_gen = []

            blks = itertools.chain(blks_disk_gen, blks_recursive_gen)

            for blk in blks:

                with blk:
                    yield blk

    def __getitem__(self, apri_n_diskonly):
        return self.get(*Register._resolve_apri_n_diskonly(apri_n_diskonly))

    def get(self, apri, n, diskonly = False, **kwargs):

        with self._time("get_elapsed"):

            self._check_open_raise("get")
            check_type(apri, "apri", ApriInfo)
            check_type(diskonly, "diskonly", bool)
            n_slice = isinstance(n, slice)

            if n_slice:

                if n.start is not None and not is_int(n.start):
                    raise TypeError("Start index of slice must be an `int`.")

                elif n.start is not None:
                    start = int(n.start)

                else:
                    start = None

                if n.stop is not None and not is_int(n.stop):
                    raise TypeError("Stop index of slice must be an `int`.")

                elif n.stop is not None:
                    stop = int(n.stop)

                else:
                    stop = None

                if n.step is not None and not is_int(n.step):
                    raise TypeError("Step index of slice must be an `int`.")

                elif n.step is not None:
                    step = int(n.step)

                else:
                    step = 1

                if n.start is not None and n.start < 0:
                    raise ValueError("Start index cannot be negative.")

                if n.stop is not None and n.stop < 0:
                    raise ValueError("Stop index cannot be negative.")

                return self._get_slice(apri, start, stop, step, diskonly, kwargs)

            else:

                n = check_return_int(n, "n")

                if not diskonly:

                    try:

                        with self._blk_by_n_ram(apri, n) as blk:
                            return blk[n]

                    except DataNotFoundError:
                        pass

                with self._reader() as ro_txn:

                    try:
                        blk_filename, startn = self._blk_by_n_pre(apri, None, True, n, ro_txn)

                    except DataNotFoundError:
                        pass

                    else:

                        with type(self)._blk_disk(blk_filename, apri, startn, False, kwargs) as blk:
                            return blk[n]

                raise DataNotFoundError(self._blk_not_found_err_msg(not diskonly, True, False, apri, None, None, n))

    def __setitem__(self, apri_n_diskonly, value):

        apri, n, diskonly = Register._resolve_apri_n_diskonly(apri_n_diskonly)
        self.set(apri, n, value, diskonly)

    def set(self, apri, n, value, diskonly = False, **kwargs):

        with self._time("get_elapsed"):

            self._check_open_raise("get")
            check_type(apri, "apri", ApriInfo)
            check_type(diskonly, "diskonly", bool)
            n = check_return_int(n, "n")

            if not diskonly:

                try:
                    blk = self._blk_by_n_ram(apri, n)

                except DataNotFoundError:
                    pass

                else:

                    blk[n] = value
                    return

            with ExitStack() as blk_stack:

                with self._reader() as ro_txn:

                    try:
                        apri_json = self._relational_encode_info(apri, ro_txn)

                    except DataNotFoundError:
                        to_raise = True

                    else:

                        try:
                            blk_filename, startn = self._blk_by_n_pre(apri, apri_json, False, n, ro_txn)

                        except DataNotFoundError:
                            to_raise = True

                        else:

                            to_raise = False
                            blk = self._blk_disk(blk_filename, apri, startn, False, kwargs)
                            blk_stack.enter_context(blk)
                            length = len(blk)
                            blk[n] = value

                            try:
                                _, _, blk_key, compressed_key, blk_filename, compressed_filename = self._rmv_disk_blk_pre(
                                    apri, apri_json, False, startn, length, ro_txn
                                )

                            except DataNotFoundError as e:
                                raise RegisterError from e # see pattern IV.4

                if to_raise:
                    raise DataNotFoundError(self._blk_not_found_err_msg(not diskonly, True, False, apri, None, None, n))

                try:
                    type(self)._rmv_disk_blk_disk2(blk_filename, compressed_filename, kwargs)

                except BaseException as e:

                    if not blk_filename.exists():
                        raise RegisterRecoveryError from e

                    else:
                        raise

                try:
                    type(self)._add_disk_blk_disk2(blk.segment(), blk_filename, False, kwargs)

                except BaseException as e:
                    raise RegisterRecoveryError from e

    def intervals(self, apri, sort = False, combine = False, diskonly = False, recursively = False):

        self._check_open_raise("intervals")
        check_type(apri, "apri", ApriInfo)
        check_type(diskonly, "diskonly", bool)
        check_type(recursively, "recursively", bool)
        intervals_ram_gen = self._intervals_ram(apri)

        with self._reader() as ro_txn:

            prefix = self._intervals_pre(apri, None, True, ro_txn)
            intervals_disk_gen = self._intervals_disk(prefix, ro_txn)
            intervals_recursive_gen = self._intervals_recursive(apri, diskonly, ro_txn)

            if not sort and not combine:

                if not diskonly:
                    yield from intervals_ram_gen

                yield from intervals_disk_gen

                if recursively:
                    yield from intervals_recursive_gen

            else:

                if not recursively:

                    if not diskonly:
                        intervals_sorted = sort_intervals(itertools.chain(intervals_ram_gen, intervals_disk_gen))

                    else:
                        intervals_sorted = intervals_disk_gen

                else:

                    if not diskonly:
                        intervals_sorted = sort_intervals(itertools.chain(
                            intervals_ram_gen, intervals_disk_gen, intervals_recursive_gen
                        ))

                    else:
                        intervals_sorted = sort_intervals(itertools.chain(intervals_disk_gen, intervals_recursive_gen))

                if combine:
                    yield from combine_intervals(intervals_sorted)

                else:
                    yield from intervals_sorted

    def total_len(self, apri, diskonly = False, recursively = False):

        self._check_open_raise("total_len")
        check_type(apri, "apri", ApriInfo)
        check_type(diskonly, "diskonly", bool)
        check_type(recursively, "recursively", bool)
        ret = 0
        to_raise = True

        if not diskonly:

            try:
                ret += self._total_len_ram(apri)

            except DataNotFoundError:
                pass

            else:
                to_raise = False

        with self._reader() as ro_txn:

            try:
                prefix = self._intervals_pre(apri, None, True, ro_txn)

            except DataNotFoundError:
                pass

            else:

                to_raise = False
                ret += self._total_len_disk(prefix, ro_txn)

            if recursively:

                try:
                    ret += self._total_len_recursive(apri, diskonly, ro_txn)

                except DataNotFoundError:
                    pass

                else:
                    to_raise = False

        if to_raise:
            raise DataNotFoundError(self._blk_not_found_err_msg(
                not diskonly, True, recursively, apri, None, None, None
            ))

        else:
            return ret

    def num_blks(self, apri, diskonly = False, recursively = False):

        self._check_open_raise("num_blks")
        check_type(apri, "apri", ApriInfo)
        check_type(diskonly, "diskonly", bool)
        check_type(recursively, "recursively", bool)

        num_blks = 0
        to_raise = True

        if not diskonly:

            try:
                num_blks += self._num_blks_ram(apri)

            except DataNotFoundError:
                pass

            else:
                to_raise = False

        with self._reader() as ro_txn:

            try:
                prefix = self._num_blks_pre(apri, None, True, ro_txn)

            except DataNotFoundError:
                pass

            else:

                num_blks += Register._num_blks_disk(prefix, ro_txn)
                to_raise = False

            if recursively:

                try:
                    num_blks += self._num_blks_recursive(apri, diskonly, ro_txn)

                except DataNotFoundError:
                    pass

                else:
                    to_raise = False

        if to_raise:
            raise DataNotFoundError(self._blk_not_found_err_msg(
                not diskonly, True, recursively, apri, None, None, None
            ))

        else:
            return num_blks

    def maxn(self, apri, diskonly = False, recursively = False):

        self._check_open_raise("maxn")
        check_type(apri, "apri", ApriInfo)
        check_type(diskonly, "diskonly", bool)
        check_type(recursively, "recursively", bool)

        ret = -1

        try:
            ret = max(ret, self._maxn_ram(apri))

        except DataNotFoundError:
            pass

        with self._reader() as ro_txn:

            try:
                prefix = self._maxn_pre(apri, None, True, ro_txn)

            except DataNotFoundError:

                if not recursively:
                    raise

            else:
                ret = max(ret, self._maxn_disk(prefix, ro_txn))

            if recursively:

                try:
                    ret = max(ret, self._maxn_recursive(apri, diskonly, ro_txn))

                except DataNotFoundError:
                    pass

        if ret >= 0:
            return ret

        else:
            raise DataNotFoundError(self._blk_not_found_err_msg(
                not diskonly, True, recursively, apri, None, None, None
            ))

    def contains_index(self, apri, n, diskonly = False, recursively = False):

        check_type(apri, "apri", ApriInfo)
        index = check_return_int(n, "index")
        check_type(diskonly, "diskonly", bool)
        check_type(recursively, "recursively", bool)
        to_raise = True

        if index < 0:
            raise ValueError("`index` must be non-negative.")

        with self._reader() as ro_txn:

            if not diskonly:

                try:
                    ret = self._contains_index_ram(apri, n)

                except DataNotFoundError:
                    pass

                else:

                    to_raise = False

                    if ret:
                        return True

            try:
                prefix = self._intervals_pre(apri, None, True, ro_txn)

            except DataNotFoundError:
                pass

            else:

                to_raise = False

                if self._contains_index_disk(prefix, n, ro_txn):
                    return True

            if recursively:

                try:
                    ret = self._contains_index_recursive(apri, n, diskonly, ro_txn)

                except DataNotFoundError:
                    pass

                else:

                    to_raise = False

                    if ret:
                        return True

            if to_raise:
                raise DataNotFoundError(self._blk_not_found_err_msg(
                    not diskonly, True, recursively, apri, None, None, None
                ))

            else:
                return False

    def contains_interval(self, apri, startn, length, diskonly = False, recursively = False):

        check_type(apri, "apri", ApriInfo)
        startn = check_return_int(startn, "startn")
        length = check_return_int(length, "length")
        check_type(diskonly, "diskonly", bool)
        check_type(recursively, "recursively", bool)

        if startn < 0:
            raise ValueError("`startn` must be non-negative.")

        if length <= 0:
            raise ValueError("`length` must be positive.")

        int_ = (startn, length)

        with self._reader() as ro_txn:

            if not diskonly:

                try:
                    ret = self._contains_interval_ram(apri, int_)

                except DataNotFoundError:
                    pass

                else:

                    to_raise = False

                    if ret:
                        return True

            try:
                prefix = self._intervals_pre(apri, None, True, ro_txn)

            except DataNotFoundError:
                pass

            else:

                to_raise = False

                if self._contains_index_disk(prefix, int_, ro_txn):
                    return True

            if recursively:

                try:
                    ret = self._contains_index_recursive(apri, int_, diskonly, ro_txn)

                except DataNotFoundError:
                    pass

                else:

                    to_raise = False

                    if ret:
                        return True

            if to_raise:
                raise DataNotFoundError(self._blk_not_found_err_msg(
                    not diskonly, True, recursively, apri, None, None, None
                ))

            else:
                return False

    #################################
    # PROTEC RAM & DISK BLK METHODS #

    def _get_slice(self, apri, start, stop, step, diskonly, kwargs):

        if start is None:

            with self._reader() as ro_txn:

                ram_start, _ = self._resolve_startn_length_ram(apri, start, None)
                disk_start, _ = self._resolve_startn_length_disk(apri, None, True, None, None, ro_txn)

                if ram_start is not None and disk_start is not None:
                    start = min(ram_start, disk_start)

                elif ram_start is not None:
                    start = ram_start

                elif disk_start is not None:
                    start = disk_start

                else:
                    return

        n = start

        if stop is not None and n >= stop:
            return

        loop_ram_and_disk = True

        while loop_ram_and_disk:
            # always check RAM `Block`s first, followed by disk `Block`s
            if not diskonly:

                loop_ram = True

                while loop_ram:

                    try:
                        blk = self._blk_by_n_ram(apri, n)

                    except DataNotFoundError:
                        loop_ram = False

                    else:

                        while n in blk and (stop is None or n < stop):

                            yield blk[n]
                            n += step

                        if stop is not None and n >= stop:
                            return

            with self._reader() as ro_txn:
                # need to refresh reader
                try:
                    blk_filename, startn = self._blk_by_n_pre(apri, None, True, n, ro_txn)

                except DataNotFoundError:
                    loop_ram_and_disk = False

                else:

                    with type(self)._blk_disk(blk_filename, apri, startn, False, kwargs) as blk:

                        while n in blk and (stop is None or n < stop):

                            yield blk[n]
                            n += step

                        if stop is not None and n >= stop:
                            return

    def _num_blks_ram(self, apri):

        if self.___contains___ram(apri):
            return len(self._ram_blks[apri])

        else:
            raise DataNotFoundError

    def _num_blks_pre(self, apri, apri_json, reencode, r_txn):

        if reencode:
            apri_json = self._relational_encode_info(apri, r_txn)

        prefix = self._get_disk_blk_prefixes(apri, apri_json, False, r_txn)[0]

        if prefix is None:
            raise DataNotFoundError(_NO_APRI_ERROR_MESSAGE.format(apri, self))

        return prefix

    @staticmethod
    def _num_blks_disk(prefix, r_txn):
        return r_txn_count_keys(prefix, r_txn)

    def _num_blks_recursive(self, apri, diskonly, r_txn):

        num_blks = 0
        to_raise = False

        for subreg, ro_txn in self._subregs_bfs(True, r_txn):

            if not diskonly:

                try:
                    num_blks += subreg._num_blks_ram(apri)

                except DataNotFoundError:
                    pass

                else:
                    to_raise = False

            try:
                num_blks += subreg._num_blks_pre(apri, None, True, ro_txn)

            except DataNotFoundError:
                pass

            else:
                to_raise = False

        if to_raise:
            raise DataNotFoundError(self._blk_not_found_err_msg(not diskonly, True, True, apri, None, None, None))

        else:
            return num_blks

    def _total_len_ram(self, apri):
        return sum(length for _, length in self._intervals_ram(apri))

    def _total_len_disk(self, prefix, r_txn):
        return sum(length for _, length in self._intervals_disk(prefix, r_txn))

    def _total_len_recursive(self, apri, diskonly, r_txn):

        ret = 0
        to_raise = True

        for subreg, ro_txn in self._subregs_bfs(True, r_txn):

            if not diskonly:

                try:
                    ret += subreg._total_len_ram(apri)

                except DataNotFoundError:
                    pass

                else:
                    to_raise = False

            try:
                prefix = subreg._intervals_pre(apri, None, True, ro_txn)

            except DataNotFoundError:
                pass

            else:

                ret += subreg._total_len_disk(prefix, ro_txn)
                to_raise = False

        if to_raise:
            raise DataNotFoundError

        else:
            return ret

    def _maxn_ram(self, apri):

        if self.___contains___ram(apri) and self._num_blks_ram(apri) > 0:

            ret = -1

            for startn, length in self._intervals_ram(apri):

                if length > 0:
                    ret = max(ret, startn + length - 1)

            if ret >= 0:
                return ret

            else:
                raise DataNotFoundError

        else:
            raise DataNotFoundError

    def _maxn_pre(self, apri, apri_json, reencode, r_txn):

        if reencode:
            apri_json = self._relational_encode_info(apri, r_txn)

        prefix, _ = self._get_disk_blk_prefixes(apri, apri_json, False, r_txn)

        for _ in self._intervals_disk(prefix, r_txn):
            return prefix

        else:
            raise DataNotFoundError(self._blk_not_found_err_msg(False, True, False, apri, None, None, None))

    def _maxn_disk(self, prefix, r_txn):

        ret = -1

        for startn, length in self._intervals_disk(prefix, r_txn):

            if length > 0:
                ret = max(ret, startn + length - 1)

        return ret

    def _maxn_recursive(self, apri, diskonly, r_txn):

        ret = -1

        for subreg, ro_txn in self._subregs_bfs(True, r_txn):

            try:

                if not diskonly:
                    ret = max(ret, subreg._maxn_ram(apri))

            except DataNotFoundError:
                pass

            try:
                prefix = subreg._maxn_pre(apri, None, True, ro_txn)

            except DataNotFoundError:
                pass

            else:
                ret = max(ret, subreg._maxn_disk(prefix, ro_txn))

        if ret >= 0:
            return ret

        else:
            raise DataNotFoundError(self._blk_not_found_err_msg(not diskonly, True, True, apri, None, None, None))

    def _blks_ram(self, apri):

        if self.___contains___ram(apri):
            yield from self._ram_blks[apri]

    def _blks_disk(self, prefix, apri, apri_json, reencode, ret_metadata, kwargs, r_txn):

        for startn, length in self._intervals_disk(prefix, r_txn):

            blk_filename, _ = self._blk_pre(apri, apri_json, reencode, startn, length, r_txn)
            yield type(self)._blk_disk(blk_filename, apri, startn, ret_metadata, kwargs)

    def _blks_recursive(self, apri, diskonly, ret_metadata, kwargs, r_txn):

        for subreg, ro_txn in self._subregs_bfs(True, r_txn):

            if not diskonly:
                yield from subreg._blks_ram(apri)

            try:
                apri_json = self._relational_encode_info(apri, r_txn)

            except DataNotFoundError:
                pass

            else:

                prefix = self._intervals_pre(apri, apri_json, False, ro_txn)
                yield from self._blks_disk(prefix, apri, apri_json, False, ret_metadata, kwargs, r_txn)

    def _blk_by_n_ram(self, apri, n):

        if self.___contains___ram(apri):

            for blk in self._blks_ram(apri):

                try:
                    blk_len = len(blk)

                except BlockNotOpenError as e:
                    raise BlockNotOpenError(_RAM_BLOCK_NOT_OPEN_ERROR_MESSAGE.format(apri, blk.startn())) from e

                if blk.startn() <= n < blk.startn() + blk_len:
                    return blk

        raise DataNotFoundError

    def _blk_by_n_pre(self, apri, apri_json, reencode, n, r_txn):

        errmsg = self._blk_not_found_err_msg(False, True, False, apri, None, None, n)

        if reencode:
            apri_json = self._relational_encode_info(apri, r_txn)

        prefix = self._intervals_pre(apri, apri_json, False, r_txn)

        for startn, length in self._intervals_disk(prefix, r_txn):

            if startn <= n < startn + length:
                break

        else:
            raise DataNotFoundError(errmsg)

        blk_key, compressed_key = self._get_disk_blk_keys(apri, apri_json, False, startn, length, r_txn)
        blk_filename, compressed_filename = self._get_disk_blk_filenames(blk_key, compressed_key, r_txn)

        if compressed_filename is not None:
            raise CompressionError(
                "Could not load disk `Block` with the following data because the `Block` is compressed. "
                "Please call `self.decompress()` first before loading the data.\n" +
                f"{apri}, startn = {startn}, length = {length}"
            )

        return blk_filename, startn

    def _blk_by_n_recursive(self, apri, n, diskonly, ret_metadata, kwargs, r_txn):

        for subreg, ro_txn in self._subregs_bfs(True, r_txn):

            if not diskonly:

                try:
                    ret = subreg._blk_by_n_ram(apri, n)

                except DataNotFoundError:
                    pass

                else:

                    if ret_metadata:
                        return ret, None

                    else:
                        return ret

            try:
                blk_filename, startn = subreg._blk_by_n_pre(apri, None, True, n, ro_txn)

            except DataNotFoundError:
                pass

            else:
                return type(subreg)._blk_disk(blk_filename, apri, startn, ret_metadata, kwargs)

        raise DataNotFoundError(self._blk_not_found_err_msg(not diskonly, True, True, apri, None, None, n))

    def _blk_ram(self, apri, startn, length, ret_metadata):

        errmsg = self._blk_not_found_err_msg(True, False, False, apri, startn, length, None)

        if self.___contains___ram(apri):

            startn_, length_ = self._resolve_startn_length_ram(apri, startn, length)

            if startn_ is None:
                raise DataNotFoundError(errmsg)

            for blk in self._blks_ram(apri):

                try:
                    blk_len = len(blk)

                except BlockNotOpenError as e:
                    raise BlockNotOpenError(_RAM_BLOCK_NOT_OPEN_ERROR_MESSAGE.format(apri, blk.startn())) from e

                if blk.startn() == startn_ and blk_len == length_:

                    if not ret_metadata:
                        return blk

                    else:
                        return blk, None

        raise DataNotFoundError(errmsg)

    def _blk_pre(self, apri, apri_json, reencode, startn, length, r_txn):

        errmsg = self._blk_not_found_err_msg(False, True, False, apri, startn, length, None)

        if reencode:
            apri_json = self._relational_encode_info(apri, r_txn)

        startn_, length_ = self._resolve_startn_length_disk(apri, apri_json, False, startn, length, r_txn)

        if startn_ is None:
            raise DataNotFoundError(errmsg)

        try:
            blk_key, compressed_key = self._get_disk_blk_keys(apri, apri_json, False, startn_, length_, r_txn)

        except TypeError:
            self._get_disk_blk_keys(apri, apri_json, False, startn_, length_, r_txn)
            raise

        if not Register._disk_blk_keys_exist(blk_key, compressed_key, r_txn):
            raise DataNotFoundError(errmsg)

        blk_filename, compressed_filename = self._get_disk_blk_filenames(blk_key, compressed_key, r_txn)

        if compressed_filename is not None:
            raise CompressionError(
                "Could not load disk `Block` with the following data because the `Block` is compressed. "
                "Please call `self.decompress()` first before loading the data.\n" +
                f"{apri}, startn = {startn_}, length = {length_}"
            )

        return blk_filename, startn_

    @classmethod
    def _blk_disk(cls, blk_filename, apri, startn, ret_metadata, kwargs):

        seg = cls.load_disk_data(blk_filename, **kwargs)
        blk = Block(seg, apri, startn)

        if not ret_metadata:
            return blk

        else:
            return blk, FileMetadata.from_path(blk_filename)

    def _blk_recursive(self, apri, startn, length, diskonly, ret_metadata, kwargs, r_txn):

        for subreg, ro_txn in self._subregs_bfs(True, r_txn):

            if not diskonly:

                try:
                    return subreg._blk_ram(apri, startn, length, ret_metadata)

                except DataNotFoundError:
                    pass

            try:
                blk_filename, _ = subreg._blk_pre(apri, None, True, startn, length, ro_txn)

            except DataNotFoundError:
                pass

            else:
                return type(subreg)._blk_disk(blk_filename, apri, startn, ret_metadata, kwargs)

        raise DataNotFoundError(self._blk_not_found_err_msg(not diskonly, True, True, apri, startn, length, None))

    @staticmethod
    def _resolve_apri_n_diskonly(apri_n_diskonly):

        if not isinstance(apri_n_diskonly, tuple) or len(apri_n_diskonly) <= 1:
            raise TypeError("Must pass at least two arguments to `reg[]`.")

        if len(apri_n_diskonly) >= 4:
            raise TypeError("Must pass at most three arguments to `reg[]`.")

        if len(apri_n_diskonly) == 2:

            apri, n = apri_n_diskonly
            diskonly = False

        else:
            apri, n, diskonly = apri_n_diskonly

        return apri, n, diskonly

    def _resolve_startn_length_ram(self, apri, startn, length):

        if startn is not None and length is not None:
            return startn, length

        if self.___contains___ram(apri) and self._num_blks_ram(apri) > 0:

            if startn is not None and length is None:

                for blk in self._blks_ram(apri):

                    try:
                        blk_len = len(blk)

                    except BlockNotOpenError as e:
                        raise BlockNotOpenError(
                            _RAM_BLOCK_NOT_OPEN_ERROR_MESSAGE.format(blk.apri(), blk.startn())
                        ) from e

                    else:

                        if blk.startn() == startn:
                            return startn, blk_len

            else:

                blk = self._ram_blks[apri][0]

                try:
                    blk_len = len(blk)

                except BlockNotOpenError as e:
                    raise BlockNotOpenError(_RAM_BLOCK_NOT_OPEN_ERROR_MESSAGE.format(blk.apri(), blk.startn())) from e

                else:
                    return blk.startn(), blk_len

        return None, None # could not resolve

    def _resolve_startn_length_disk(self, apri, apri_json, reencode, startn, length, r_txn):

        if startn is not None and length is not None:
            return startn, length

        try:

            if reencode:
                apri_json = self._relational_encode_info(apri, r_txn)

        except DataNotFoundError:
            return None, None

        else:

            if startn is not None and length is None:
                prefix = self._get_disk_blk_prefixes_startn(apri, apri_json, False, startn, r_txn)[0]

            else:
                prefix = self._get_disk_blk_prefixes(apri, apri_json, False, r_txn)[0]

            with r_txn_prefix_iter(prefix, r_txn) as it:

                for key, _ in it:
                    return self._get_startn_length(_BLK_KEY_PREFIX_LEN, key)

            return None, None # could not resolve

    def _intervals_ram(self, apri):

        if self.___contains___ram(apri):

            for blk in self._blks_ram(apri):

                try:
                    blk_len = len(blk)

                except BlockNotOpenError as e:
                    raise BlockNotOpenError(_RAM_BLOCK_NOT_OPEN_ERROR_MESSAGE.format(apri, blk.startn())) from e

                else:
                    yield blk.startn(), blk_len

    def _intervals_pre(self, apri, apri_json, reencode, r_txn):

        if reencode:
            apri_json = self._relational_encode_info(apri, r_txn)

        return self._get_disk_blk_prefixes(apri, apri_json, False, r_txn)[0]

    def _intervals_disk(self, prefix, r_txn):

        if prefix is not None:

            with r_txn_prefix_iter(prefix, r_txn) as it:

                for key, _ in it:
                    yield self._get_startn_length(_BLK_KEY_PREFIX_LEN, key)

    def _intervals_recursive(self, apri, diskonly, r_txn):

        for subreg, ro_txn in self._subregs_bfs(True, r_txn):

            if not diskonly:
                yield from subreg._intervals_ram(apri)

            try:
                prefix = subreg._intervals_pre(apri, None, True, ro_txn)

            except DataNotFoundError:
                pass

            else:
                yield from subreg._intervals_disk(prefix, r_txn)

    def _contains_index_ram(self, apri, n):

        for startn, length in self._intervals_ram(apri):

            if startn <= n < startn + length:
                return True

        else:
            return False

    def _contains_index_disk(self, prefix, n, r_txn):

        for startn, length in self._intervals_disk(prefix, r_txn):

            if startn <= n < startn + length:
                return True

        else:
            return False

    def _contains_index_recursive(self, apri, n, diskonly, r_txn):

        for subreg, ro_txn in self._subregs_bfs(True, r_txn):

            if not diskonly and subreg._contains_index_ram(apri, n):
                return True

            try:
                prefix = self._intervals_pre(apri, None, True, r_txn)

            except DataNotFoundError:
                pass

            else:

                if self._contains_index_disk(prefix, n, r_txn):
                    return True

        raise DataNotFoundError

    def _contains_interval_ram(self, apri, int_):

        for int__ in combine_intervals(self._intervals_ram(apri)):

            if intervals_subset(int_, int__):
                return True

        else:
            return False

    def _contains_interval_disk(self, prefix, int_, r_txn):

        for int__ in combine_intervals(self._intervals_disk(prefix, r_txn)):

            if intervals_subset(int_, int__):
                return True

        else:
            return False

    def _contains_interval_recursive(self, apri, int_, diskonly, r_txn):

        for subreg, ro_txn in self._subregs_bfs(True, r_txn):

            if not diskonly and subreg._contains_interval_ram(apri, int_):
                return True

            try:
                prefix = self._intervals_pre(apri, None, True, r_txn)

            except DataNotFoundError:
                pass

            else:

                if self._contains_interval_disk(prefix, int_, r_txn):
                    return True

        raise DataNotFoundError

    def _blk_not_found_err_msg(self, ram, disk, recursive, apri, startn, length, n):

        if ram and disk:
            type_ = "disk nor RAM"

        elif disk:
            type_ = "disk"

        elif ram:
            type_ = "RAM"

        else:
            raise ValueError

        if recursive:
            msg = (
                f"No {type_} `Block` found in the following `Register`, nor in any of its subregisters, with the "
                f"following data :\n{apri}"
            )

        else:
            msg = f"No {type_} `Block` found in the following `Register` with the following data :\n{apri}"

        if n is not None:
            return f"{msg}\nn = {n}\n{self}"

        elif startn is not None and length is None:
            return f"{msg}\nstartn = {startn}\n{self}"

        elif startn is not None and length is not None:
            return f"{msg}\nstartn = {startn}\nlength = {length}\n{self}"

        else:
            return msg

class PickleRegister(Register, file_suffix = ".pickle"):

    @classmethod
    def dump_disk_data(cls, data, filename, **kwargs):

        with filename.open("wb") as fh:
            pickle.dump(data, fh)

    @classmethod
    def load_disk_data(cls, filename, **kwargs):

        with filename.open("rb") as fh:
            return pickle.load(fh), filename

class NumpyRegister(Register, file_suffix = ".npy"):

    @classmethod
    def dump_disk_data(cls, data, filename, **kwargs):
        np.save(filename, data, allow_pickle = False, fix_imports = False)

    @classmethod
    def load_disk_data(cls, filename, **kwargs):

        if "mmap_mode" in kwargs:
            mmap_mode = kwargs["mmap_mode"]

        else:
            mmap_mode = None

        NumpyRegister._check_mmap_mode_raise(mmap_mode)
        return np.load(filename, mmap_mode = mmap_mode, allow_pickle = False, fix_imports = False)

    @classmethod
    def clean_disk_data(cls, filename, **kwargs):

        if len(kwargs) > 0:
            raise KeyError("This method accepts no keyword-arguments.")

        return Register.clean_disk_data(filename)

    @staticmethod
    def _check_mmap_mode_raise(mmap_mode):

        if mmap_mode not in [None, "r+", "r", "w+", "c"]:
            raise ValueError(
                "The keyword-argument `mmap_mode` for `Numpy_Register.blk` can only have the values " +
                "`None`, 'r+', 'r', 'w+', 'c'. Please see " +
                "https://numpy.org/doc/stable/reference/generated/numpy.memmap.html#numpy.memmap for more information."
            )

    def set(self, apri, n, value, diskonly = False, **kwargs):

        mmap_mode = kwargs.get("mmap_mode", None)
        self._check_mmap_mode_raise(mmap_mode)

        if mmap_mode != "r+":
            super().set(apri, n, value, diskonly, **kwargs)

        else:

            if not diskonly:

                try:
                    blk = self._blk_by_n_ram(apri, n)

                except DataNotFoundError:
                    pass

                else:

                    blk[n] = value
                    return

            with self._reader() as ro_txn:

                try:
                    blk_filename, startn = self._blk_by_n_pre(apri, None, True, n, ro_txn)

                except DataNotFoundError:
                    pass

                else:

                    with self._blk_disk(blk_filename, apri, startn, False, kwargs) as blk:

                        blk[n] = value
                        return

            raise DataNotFoundError(self._blk_not_found_err_msg(not diskonly, True, False, apri, None, None, n))

    @contextmanager
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

        with super().blk(apri, startn, length, diskonly, recursively, ret_metadata, **kwargs) as ret:

            if ret_metadata:
                blk = ret[0]

            else:
                blk = ret

            if issubclass(blk.segment_type, np.memmap):

                with MemmapBlock.cast(blk) as blk:

                    if ret_metadata:
                        yield blk, ret[1]

                    else:
                        yield blk

            else:

                if ret_metadata:
                    yield blk, ret[1]

                else:
                    yield blk

    def concat_disk_blks(self, apri, startn = None, length = None, delete = False, ret_metadata = False, **kwargs):

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

        with self._reader() as ro_txn:
            ret = self._concat_disk_blks_pre(apri, None, True, startn, length, ro_txn)

        combined_already, combined_blk_key, combined_compressed_key, combined_filename, combined_seg, del_keys, del_filenames = ret
        rrw_txn = None

        if combined_already:

            if ret_metadata:
                return FileMetadata.from_path(combined_filename)

            else:
                return None

        try:

            with self._reversible_writer() as rrw_txn:
                self._concat_disk_blks_disk(
                    combined_blk_key, combined_compressed_key, combined_filename, del_keys, delete, rrw_txn
                )

            return self._concat_disk_blks_disk2(
                combined_seg, combined_filename, del_filenames, ret_metadata, delete, kwargs
            )

        except BaseException as e:

            if rrw_txn is not None:

                with self._writer() as rw_txn:
                    ee = self._concat_disk_blks_error(combined_filename, del_filenames, rrw_txn, rw_txn, e)

                raise ee

            else:
                raise

    def _concat_disk_blks_pre(self, apri, apri_json, reencode, startn, length, r_txn):

        if reencode:
            apri_json = self._relational_encode_info(apri, r_txn)

        res_startn, _ = self._resolve_startn_length_disk(apri, apri_json, False, startn, length, r_txn)
        prefix = self._intervals_pre(apri, apri_json, False, r_txn)

        if length is None:
            # infer length
            current_segment = False
            res_length = 0

            for startn_, length_ in self._intervals_disk(prefix, r_txn):

                if length_ > 0:

                    if current_segment:

                        if res_startn > startn_:
                            raise RuntimeError("Could not infer a value for `length`.")

                        elif res_startn == startn_:
                            raise ValueError(
                                f"Overlapping `Block` intervals found with {str(apri)}."
                            )

                        else:

                            if res_startn + res_length > startn_:
                                raise ValueError(
                                    f"Overlapping `Block` intervals found with {str(apri)}."
                                )

                            elif res_startn + res_length == startn_:
                                res_length += length_

                            else:
                                break

                    else:

                        if res_startn < startn_:
                            raise DataNotFoundError(self._blk_not_found_err_msg(
                                False, True, False, apri, startn, None, None
                            ))

                        elif res_startn == startn_:

                            res_length += length_
                            current_segment = True

            if res_length == 0:
                raise RuntimeError("could not infer a value for `length`.")

            warnings.warn(f"`length` value not specified, inferred value: `length = {res_length}`.")

        else:
            res_length = length

        combined_interval = None
        last_check = False
        last_startn_ = None
        startn_ = None
        length_ = None
        intervals_to_get = []
        del_filenames = []
        del_keys = []

        for startn_, length_ in self._intervals_disk(prefix, r_txn):
            # infer blocks to combine
            if last_check:

                if last_startn_ == startn_ and length_ > 0:
                    raise ValueError(f"Overlapping `Block` intervals found with {str(apri)}.")

                else:
                    break

            if length_ > 0:

                last_startn_ = startn_

                if startn_ < res_startn and res_startn < startn_ + length_:
                    raise ValueError(
                        f"The first `Block` does not have the right size. Try again by calling "
                        f"`reg.concat_disk_blks({str(apri)}, {startn_}, {res_length - (startn_ - res_startn)})`."
                    )

                elif startn_ >= res_startn:

                    if combined_interval is None:

                        if startn_ > res_startn:
                            raise DataNotFoundError(
                                self._blk_not_found_err_msg(False, True, False, apri, startn, None, None)
                            )

                        elif startn_ != res_startn:
                            raise RuntimeError("Something went wrong trying to combine `Block`s.")

                        else:
                            combined_interval = (startn_, length_)

                    else:

                        sum_combined_interval = sum(combined_interval)

                        if startn_ > sum_combined_interval:
                            raise DataNotFoundError(self._blk_not_found_err_msg(
                                False, True, False, apri, sum_combined_interval, startn_ - sum_combined_interval,
                                None
                            ))

                        elif startn_ == sum_combined_interval and startn_ + length_ > res_startn + res_length:
                            raise ValueError(
                                f"The last `Block` does not have the right size. Try again by calling "
                                f"`reg.concat_disk_blks({str(apri)}, {res_startn}, "
                                f"{res_length - (startn_ + length_ - (res_startn + res_length))})`."
                            )

                        elif startn_ != sum_combined_interval:
                            raise ValueError(f"Overlapping `Block` intervals found with {str(apri)}.")

                        else:
                            combined_interval = (res_startn, combined_interval[1] + length_)

                    intervals_to_get.append((startn_, length_))
                    keys = self._get_disk_blk_keys(apri, apri_json, False, startn_, length_, r_txn)
                    del_filenames.append(self._get_disk_blk_filenames(keys[0], keys[1], r_txn))
                    del_keys.extend(keys)
                    last_check = startn_ + length_ == res_startn + res_length

        else:

            if startn_ is None:
                raise DataNotFoundError(self._blk_not_found_err_msg(False, True, False, apri, None, None, None))

            elif startn_ + length_ != res_startn + res_length:
                raise ValueError(
                    f"The last `Block` does not have the right size. "
                    f"Try again by calling `reg.concat_disk_blks(apri, {res_startn}, {startn_ + length_})`."
                )

        if len(intervals_to_get) == 1:
            return True, None, None, del_filenames[0][0], None, None, None

        blks = []
        metadata = []
        fixed_shape = None
        ref_blk_startn = None
        ref_blk_len = None

        with ExitStack() as stack:
            # All blocks will be opened and will remain open until they are concatenated
            for startn_, length_ in intervals_to_get:

                blk = stack.enter_context(self.blk(apri, startn_, length_, True, False, False, mmap_mode="r"))
                blks.append(blk)
                metadata.append((startn_, length_))
                # check that all shapes are correct
                if fixed_shape is None:
                    # initialize correct shape
                    fixed_shape = blk.segment().shape[1:]
                    ref_blk_startn = blk.startn()
                    ref_blk_len = len(blk)

                elif fixed_shape != blk.segment().shape[1:]:
                    raise ValueError(
                        "Cannot combine the following two `Block`s because all axes other than axis 0 must have the"
                        " same shape:\n"
                        f"{str(apri)}, startn = {ref_blk_startn}, length = {ref_blk_len}\n, shape = "
                        f"{str(fixed_shape)}\n"
                        f"{str(apri)}, startn = {startn_}, length = {length_}\n, shape = "
                        f"{str(blk.segment().shape)}"
                    )

            combined_seg = np.concatenate([blk.segment() for blk in blks], axis=0)

        combined_blk_key, combined_compressed_key, combined_filename, _ = self._add_disk_blk_pre(
            apri, apri_json, False, res_startn, res_length, False, True, r_txn
        )
        return False, combined_blk_key, combined_compressed_key, combined_filename, combined_seg, del_keys, del_filenames

    def _concat_disk_blks_disk(
        self, combined_blk_key, combined_compressed_key, combined_filename, del_keys, delete, rw_txn
    ):

        if delete:

            for key in del_keys:
                rw_txn.delete(key)

        self._add_disk_blk_disk(
            None, None, None, combined_blk_key, combined_compressed_key, combined_filename, False, rw_txn
        )

    @classmethod
    def _concat_disk_blks_disk2(cls, seg, blk_filename, del_filenames, ret_metadata, delete, kwargs):

        ret = cls._add_disk_blk_disk2(seg, blk_filename, ret_metadata, kwargs)

        if delete:

            for blk_filename, compressed_filename in del_filenames:
                cls._rmv_disk_blk_disk2(blk_filename, compressed_filename, kwargs)

        return ret

    def _concat_disk_blks_error(self, blk_filename, del_filenames, rrw_txn, rw_txn, e):

        no_recover = RegisterRecoveryError(
            f"The following `Register` failed to recover from `concat_disk_blks` :\n{self}."
        )
        no_recover.__cause__ = e

        if isinstance(e, RegisterRecoveryError):
            return e

        try:

            try:
                type(self).clean_disk_data(blk_filename)

            except FileNotFoundError:
                pass

            for blk_filename, compressed_filename in del_filenames:

                if compressed_filename is not None:

                    if compressed_filename.exists():

                        try:
                            blk_filename.touch()

                        except FileExistsError:
                            pass

                    else:
                        return no_recover

                elif not blk_filename.exists():
                    return no_recover

            rrw_txn.reverse(rw_txn)
            return e

        except BaseException as ee:

            no_recover.__cause__ = ee
            return no_recover

class _CopyRegister(Register):

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

class _RelationalApriInfoStrHook:

    def __init__(self, reg, r_txn):

        self._reg = reg
        self._r_txn = r_txn

    def __call__(self, cls, str_):

        if cls == ApriInfo:

            id_ = str_[len(ApriInfo.__name__) : ]

            if len(id_) == _MAX_NUM_APRI_LEN:

                try:
                    int(id_)

                except ValueError:
                    return str_

                else:
                    return json.JSONDecoder().decode(
                        self._reg._get_apri_json(id_.encode("ASCII"), self._r_txn).decode("ASCII")
                    )

            else:
                return str_

        else:
            return cls._default_str_hook(str_)

class _RelationalInfoJsonEncoder(_InfoJsonEncoder):

    def __init__(self, reg, r_txn, *args, **kwargs):

        self._reg = reg
        self._r_txn = r_txn
        super().__init__(*args, **kwargs)

    def default(self, obj):

        if isinstance(obj, ApriInfo):
            return ApriInfo.__name__ + self._reg._get_apri_id(obj, None, True, self._r_txn).decode("ASCII")

        else:
            return super().default(obj)
