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

from cornifer.errors import DataNotFoundError, RegisterAlreadyOpenError, RegisterError, CompressionError, \
    DecompressionError, NOT_ABSOLUTE_ERROR_MESSAGE, RegisterRecoveryError
from cornifer.info import ApriInfo, AposInfo
from cornifer.blocks import Block, MemmapBlock
from cornifer.filemetadata import FileMetadata
from cornifer._utilities import intervalsOverlap, randomUniqueFilename, isInt, \
    resolvePath, BYTES_PER_MB, isDeletable
from cornifer._utilities.lmdb import lmdbHasKey, lmdbPrefixIter, openLmdb, lmdbIsClosed, lmdbCountKeys
from cornifer.regfilestructure import VERSION_FILEPATH, LOCAL_DIR_CHARS, \
    COMPRESSED_FILE_SUFFIX, MSG_FILEPATH, CLS_FILEPATH, checkRegStructure, DATABASE_FILEPATH, \
    REG_FILENAME
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
    "No disk block found with the following data: {0}, startn = {1}, length = {2}."
)

_DISK_BLOCK_DATA_NOT_FOUND_ERROR_MSG_N = (
    "No disk block found with the following data: {0}, n = {1}."
)

_RAM_BLOCK_DATA_NOT_FOUND_ERROR_MSG_FULL = (
    "No RAM block found with the following data: {0}, startn = {1}, length = {2}."
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
    "Exceeded max `Register` size of {0} Bytes. Please increase the max size using the method `increaseRegSize`."
)

#################################
#           CONSTANTS           #

_START_N_TAIL_LENGTH_DEFAULT   = 12
_START_N_HEAD_DEFAULT          =  0
_INITIAL_REGISTER_SIZE_DEFAULT = 5 * BYTES_PER_MB

class Register(ABC):

    #################################
    #     PUBLIC INITIALIZATION     #

    def __init__(self, savesDir, msg, initialRegSize = None):
        """
        :param savesDir: (type `str`)
        :param msg: (type `str`) A brief message describing the data associated to this `Register`.
        :param initialRegSize: (type `int`, default 5) Size in bytes. You may wish to set this lower
        than 5 MB if you do not expect to add many disk `Block`s to your register and you are concerned about disk
        memory. If your `Register` exceeds `initial_register_size`, then you can adjust the database size later via the
        method `increaseRegSize`. If you are on a non-Windows system, there is no harm in setting this value
        to be very large (e.g. 1 TB).
        """

        if not isinstance(savesDir, (str, Path)):
            raise TypeError("`savesDir` must be a string or a `pathlib.Path`.")

        if not isinstance(msg, str):
            raise TypeError("`msg` must be a string.")

        if initialRegSize is not None and not isInt(initialRegSize):
            raise TypeError("`initialRegSize` must be of type `int`.")

        elif initialRegSize is not None:
            initialRegSize = int(initialRegSize)

        else:
            initialRegSize = _INITIAL_REGISTER_SIZE_DEFAULT

        if initialRegSize <= 0:
            raise ValueError("`initialRegSize` must be positive.")

        self.savesDir = resolvePath(Path(savesDir))

        if not self.savesDir.is_dir():
            raise FileNotFoundError(
                f"You must create the file `{str(self.savesDir)}` before calling " +
                f"`{self.__class__.__name__}(\"{str(self.savesDir)}\", \"{msg}\")`."
            )

        self._msg = msg
        self._msgFilepath = None

        self._localDir = None
        self._localDirBytes = None
        self._subregBytes = None

        self._db = None
        self._dbFilepath = None
        self._dbMapSize = initialRegSize
        self._readonly = None

        self._version = CURRENT_VERSION
        self._versionFilepath = None

        self._clsFilepath = None

        self._startnHead = _START_N_HEAD_DEFAULT
        self._startnTailLength = _START_N_TAIL_LENGTH_DEFAULT
        self._startnTailMod = 10 ** self._startnTailLength

        self._ramBlks = []

        self._created = False

    @staticmethod
    def addSubclass(subclass):

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
    def _fromLocalDir(localDir):
        """Return a `Register` instance from a `localDir` with the correct concrete subclass.

        This static method does not open the `Register` database at any point.

        :param localDir: (type `pathlib.Path`) Absolute.
        :return: (type `Register`)
        """

        if not localDir.is_absolute():
            raise ValueError(NOT_ABSOLUTE_ERROR_MESSAGE.format(str(localDir)))

        if not localDir.exists():
            raise FileNotFoundError(f"The `Register` database `{str(localDir)}` could not be found.")

        checkRegStructure(localDir)

        if Register._instanceExists(localDir):

            # return the `Register` that has already been opened
            return Register._getInstance(localDir)

        else:

            with (localDir / CLS_FILEPATH).open("r") as fh:
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
                    f"`Register` via `Register.addSubclass({cls_name})`."
                )

            with (localDir / MSG_FILEPATH).open("r") as fh:
                msg = fh.read()

            reg = con(localDir.parent, msg)

            reg._setLocalDir(localDir)

            with (localDir / VERSION_FILEPATH).open("r") as fh:
                reg._version = fh.read()

            return reg

    @staticmethod
    def _addInstance(localDir, reg):
        """
        :param localDir: (type `pathlib.Path`) Absolute.
        :param reg: (type `Register`)
        """

        if not localDir.is_absolute():
            raise ValueError(NOT_ABSOLUTE_ERROR_MESSAGE.format(str(localDir)))

        Register._instances[localDir] = reg

    @staticmethod
    def _instanceExists(localDir):
        """
        :param localDir: (type `pathlib.Path`) Absolute.
        :return: (type `bool`)
        """

        if not localDir.is_absolute():
            raise ValueError(NOT_ABSOLUTE_ERROR_MESSAGE.format(str(localDir)))

        return localDir in Register._instances.keys()

    @staticmethod
    def _getInstance(localDir):
        """
        :param localDir: (type `pathlib.Path`) Absolute.
        :return: (type `Register`)
        """

        if not localDir.is_absolute():
            raise ValueError(NOT_ABSOLUTE_ERROR_MESSAGE.format(str(localDir)))

        return Register._instances[localDir]

    #################################
    #    PUBLIC REGISTER METHODS    #

    def __eq__(self, other):

        if not self._created or not other._created:
            raise RegisterError(_NOT_CREATED_ERROR_MESSAGE.format("__eq__"))

        elif type(self) != type(other):
            return False

        else:
            return self._localDir == other._localDir

    def __hash__(self):

        if not self._created:
            raise RegisterError(_NOT_CREATED_ERROR_MESSAGE.format("__hash__"))

        else:
            return hash(str(self._localDir)) + hash(type(self))

    def __str__(self):
        return self._msg

    def __repr__(self):
        return f"{self.__class__.__name__}(\"{str(self.savesDir)}\", \"{self._msg}\")"

    def __contains__(self, apri):

        self._checkOpenRaise("__contains__")

        if any(blk.apri() == apri for blk in self._ramBlks):
            return True

        else:
            return lmdbHasKey(self._db, _APRI_ID_KEY_PREFIX + apri.toJson().encode("ASCII"))

    def __iter__(self):
        return iter(self.apriInfos())

    def setMsg(self, message):
        """Give this `Register` a brief description.

        WARNING: This method OVERWRITES the current msg. In order to append a new msg to the current one, do
        something like the following:

            old_message = str(reg)
            new_message = old_message + " Hello!"
            reg.setMsg(new_message)

        :param message: (type `str`)
        """

        if not isinstance(message, str):
            raise TypeError("`msg` must be a string.")

        self._msg = message

        if self._created:
            with self._msgFilepath.open("w") as fh:
                fh.write(message)

    def setStartnInfo(self, head = None, tailLen = None):
        """Set the range of the `startn` parameters of disk `Block`s belonging to this `Register`.

        Reset to default `head` and `tail_length` by omitting the parameters.

        If the `startn` parameter is very large (of order more than trillions), then the `Register` database can
        become very bloated by storing many redundant digits for the `startn` parameter. Calling this method with
        appropriate `head` and `tail_length` parameters alleviates the bloat.

        The "head" and "tail" of a non-negative number x is defined by x = head * 10^L + tail, where L is the "length",
        or the number of digits, of "tail". (L must be at least 1, and 0 is considered to have 1 digit.)

        By calling `setstartnInfo(head, tail_length)`, the user is asserting that the startn of every disk
        `Block` belong to this `Register` can be decomposed in the fashion startn = head * 10^tail_length + tail. The
        user is discouraged to call this method for large `tail_length` values (>12), as this is likely unnecessary and
        defeats the purpose of this method.

        :param head: (type `int`, optional) Non-negative. If omitted, resets this `Register` to the default `head`.
        :param tailLen: (type `int`) Positive. If omitted, resets this `Register` to the default `tail_length`.
        """

        # DEBUG : 1, 2

        self._checkOpenRaise("setstartnInfo")

        self._checkReadwriteRaise("setstartnInfo")

        if head is not None and not isInt(head):
            raise TypeError("`head` must be of type `int`.")

        elif head is not None:
            head = int(head)

        else:
            head = _START_N_HEAD_DEFAULT

        if tailLen is not None and not isInt(tailLen):
            raise TypeError("`tailLen` must of of type `int`.")

        elif tailLen is not None:
            tailLen = int(tailLen)

        else:
            tailLen = _START_N_TAIL_LENGTH_DEFAULT

        if head < 0:
            raise ValueError("`head` must be non-negative.")

        if tailLen <= 0:
            raise ValueError("`tailLen` must be positive.")

        if head == self._startnHead and tailLen == self._startnTailLength:
            return

        new_mod = 10 ** tailLen

        with lmdbPrefixIter(self._db, _BLK_KEY_PREFIX) as it:

            for key, _ in it:

                apri, startn, length = self._convertDiskBlockKey(_BLK_KEY_PREFIX_LEN, key)

                if startn // new_mod != head:

                    raise ValueError(
                        "The following `startn` does not have the correct head:\n" +
                        f"`startn`   : {startn}\n" +
                        "That `startn` is associated with a `Block` whose `ApriInfo` and length is:\n" +
                        f"`ApriInfo` : {str(apri.toJson())}\n" +
                        f"length      : {length}\n"
                    )

        if _debug == 1:
            raise KeyboardInterrupt

        try:

            with self._db.begin(write = True) as rw_txn:

                with self._db.begin() as ro_txn:

                    with lmdbPrefixIter(ro_txn, _BLK_KEY_PREFIX) as it:

                        rw_txn.put(_START_N_HEAD_KEY, str(head).encode("ASCII"))
                        rw_txn.put(_START_N_TAIL_LENGTH_KEY, str(tailLen).encode("ASCII"))

                        for key, val in it:

                            _, startn, _ = self._convertDiskBlockKey(_BLK_KEY_PREFIX_LEN, key)
                            apri_json, _, length_bytes = Register._splitDiskBlockKey(_BLK_KEY_PREFIX_LEN, key)

                            new_startn_bytes = str(startn % new_mod).encode("ASCII")

                            new_key = Register._joinDiskBlockData(
                                _BLK_KEY_PREFIX, apri_json, new_startn_bytes, length_bytes
                            )

                            if key != new_key:

                                rw_txn.put(new_key, val)
                                rw_txn.delete(key)

                if _debug == 2:
                    raise KeyboardInterrupt

        except lmdb.MapFullError as e:
            raise RegisterError(_MEMORY_FULL_ERROR_MESSAGE.format(self._dbMapSize)) from e

        self._startnHead = head
        self._startnTailLength = tailLen
        self._startnTailMod = 10 ** self._startnTailLength

    @contextmanager
    def open(self, readonly = False):

        if not self._created and not readonly:

            # set local directory info and create levelDB database
            localDir = randomUniqueFilename(self.savesDir, length = 4, alphabet = LOCAL_DIR_CHARS)

            try:

                localDir.mkdir(exist_ok = False)
                (localDir / REG_FILENAME).mkdir(exist_ok = False)

                with (localDir / MSG_FILEPATH).open("x") as fh:
                    fh.write(self._msg)

                with (localDir / VERSION_FILEPATH).open("x") as fh:
                    fh.write(self._version)

                with (localDir / CLS_FILEPATH).open("x") as fh:
                    fh.write(str(type(self).__name__))

                (localDir / DATABASE_FILEPATH).mkdir(exist_ok = False)

                self._setLocalDir(localDir)

                self._db = openLmdb(self._dbFilepath, self._dbMapSize, False)

                try:

                    with self._db.begin(write = True) as txn:
                        # set register info
                        txn.put(_START_N_HEAD_KEY, str(self._startnHead).encode("ASCII"))
                        txn.put(_START_N_TAIL_LENGTH_KEY, str(self._startnTailLength).encode("ASCII"))
                        txn.put(_CURR_ID_KEY, b"0")

                except lmdb.MapFullError as e:
                    raise RegisterError(_MEMORY_FULL_ERROR_MESSAGE.format(self._dbMapSize)) from e

                Register._addInstance(localDir, self)
                yiel = self

            except BaseException as e:

                if localDir.is_dir():
                    shutil.rmtree(localDir)

                raise e

        elif self._created:
            yiel = self._openCreated(readonly)

        else:
            raise ValueError(
                "You must `open` this `Register` at least once with `readonly = False` before you can open it in "
                "read-only mode."
            )

        try:
            yield yiel

        finally:
            yiel._closeCreated()

    def increaseRegSize(self, numBytes):
        """WARNING: DO NOT CALL THIS METHOD FROM MORE THAN ONE PYTHON PROCESS AT A TIME. You are safe if you call it
        from only one Python process. You are safe if you have multiple Python processes running and call it from only
        ONE of them. But do NOT call it from multiple processes at once. Doing so may result in catastrophic loss of
        data.

        :param numBytes: (type `int`) Positive.
        """

        self._checkOpenRaise("increaseRegSize")

        if not isInt(numBytes):
            raise TypeError("`numBytes` must be of type `int`.")

        if numBytes <= 0:
            raise ValueError("`numBytes` must be positive.")

        if numBytes <= self._dbMapSize:
            raise ValueError("`numBytes` must be larger than the current `Register` size.")

        self._db.set_mapsize(numBytes)
        self._dbMapSize = numBytes

    def regSize(self):
        return self._dbMapSize

    def ident(self):

        if not self._created:
            raise RegisterError(_NOT_CREATED_ERROR_MESSAGE.format("ident"))

        return str(self._localDir)

    #################################
    #    PROTEC REGISTER METHODS    #

    def _openCreated(self, readonly):

        if Register._instanceExists(self._localDir):
            ret = Register._getInstance(self._localDir)
        else:
            ret = self

        if not ret._created:
            raise RegisterError(_NOT_CREATED_ERROR_MESSAGE.format("_openCreated"))

        if ret._db is not None and not ret._dbIsClosed():
            raise RegisterAlreadyOpenError()

        self._readonly = readonly

        ret._db = openLmdb(self._dbFilepath, self._dbMapSize, readonly)

        return ret

    def _closeCreated(self):
        self._db.close()

    @contextmanager
    def _recursiveOpen(self, readonly):

        if not self._created:
            raise RegisterError(_NOT_CREATED_ERROR_MESSAGE.format("_recursiveOpen"))

        else:

            try:
                yiel = self._openCreated(readonly)
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
                    yiel._closeCreated()

    def _checkOpenRaise(self, methodName):

        if self._db is None or self._dbIsClosed():
            raise RegisterError(
                f"This `Register` database has not been opened. You must open this register via `with reg.open() as " +
                f"reg:` before calling the method `{methodName}`."
            )

    def _checkReadwriteRaise(self, methodName):
        """Call `self._checkOpenRaise` before this method."""

        if self._readonly:
            raise RegisterError(
                f"This `Register` is `open`ed in read-only mode. In order to call the method `{methodName}`, you must "
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
    #     if current_size + entry_size_bytes >= Register._MEMORY_FULL_PROP * self._dbMapSize:
    #
    #         raise MemoryError(
    #             "The `Register` database is out of memory. Please allocate more memory using the method "
    #             "`Register.increaseRegSize`."
    #         )

    def _setLocalDir(self, localDir):
        """`localDir` and a corresponding register database must exist prior to calling this method.

        :param localDir: (type `pathlib.Path`) Absolute.
        """

        if not localDir.is_absolute():
            raise ValueError(NOT_ABSOLUTE_ERROR_MESSAGE.format(str(localDir)))

        if localDir.parent != self.savesDir:
            raise ValueError(
                "The `localDir` argument must be a sub-directory of `reg.savesDir`.\n" +
                f"`localDir.parent`    : {str(localDir.parent)}\n"
                f"`reg.savesDir` : {str(self.savesDir)}"
            )

        checkRegStructure(localDir)

        self._created = True

        self._localDir = localDir
        self._localDirBytes = str(self._localDir).encode("ASCII")

        self._dbFilepath = self._localDir / DATABASE_FILEPATH

        self._subregBytes = (
            _SUB_KEY_PREFIX + self._localDirBytes
        )

        self._versionFilepath = localDir / VERSION_FILEPATH
        self._msgFilepath = localDir / MSG_FILEPATH
        self._clsFilepath = localDir / CLS_FILEPATH

    def _hasCompatibleVersion(self):
        return self._version in COMPATIBLE_VERSIONS

    def _dbIsClosed(self):

        if not self._created:
            raise RegisterError(_NOT_CREATED_ERROR_MESSAGE.format("_dbIsClosed"))

        else:
            return lmdbIsClosed(self._db)

    #################################
    #      PUBLIC APRI METHODS      #

    def apriInfos(self, recursively = False):

        if not isinstance(recursively, bool):
            raise TypeError("`recursively` must be of type `bool`")

        ret = []
        for blk in self._ramBlks:
            ret.append(blk.apri())

        self._checkOpenRaise("apriInfos")

        with lmdbPrefixIter(self._db, _ID_APRI_KEY_PREFIX) as it:
            for _, val in it:
                ret.append(ApriInfo.fromJson(val.decode("ASCII")))


        if recursively:
            for subreg in self._iterSubregs():
                with subreg._recursiveOpen(True) as subreg:
                    ret.append(subreg.apriInfos())

        return sorted(list(set(ret)))

    def changeApriInfo(self, oldApri, newApri, recursively = False):
        """Replace an old `ApriInfo`, and all references to it, with a new `ApriInfo`.

        If ANY `Block`, `ApriInfo`, or `AposInfo` references `old_apri`, its entries in this `Register` will be
        updated to reflect the replacement of `old_apri` with `new_apri`. (See example below.) After the replacement
        `old_apri` -> `new_apri` is made, the set of `ApriInfo` that changed under that replacement must be disjoint
        from the set of `ApriInfo` that did not change. Otherwise, a `ValueError` is raised.

        For example, say we intend to replace

        `old_apri = ApriInfo(descr = "periodic points")`

        with

        `new_apri = ApriInfo(descr = "odd periods", ref = "Newton et al. 2005")`.

        In an example `Register`, there are two `Block`s, one with `old_apri` and the other with

        `some_other_apri = ApriInfo(descr = "period length", respective = old_apri)`.

        After a call to `changeApriInfo(old_apri, new_apri)`, the first `Block` will have `new_apri` and the second
        will have

        `ApriInfo(descr = "period length", respective = new_apri)`.

        :param oldApri: (type `ApriInfo`)
        :param newApri: (type `ApriInfo`)
        :param recursively: (type `bool`)
        :raise ValueError: See above.
        """

        # DEBUG : 1, 2, 3

        self._checkOpenRaise("changeApriInfo")

        self._checkReadwriteRaise("changeApriInfo")

        # raises `DataNotFoundError` if `oldApri` does not have an ID
        old_apri_id = self._getIdByApri(oldApri, None, False)

        if oldApri == newApri:
            return

        old_apri_json = oldApri.toJson().encode("ASCII")

        old_apri_id_key = _APRI_ID_KEY_PREFIX + old_apri_json
        old_id_apri_key = _ID_APRI_KEY_PREFIX + old_apri_id

        new_apri_json = newApri.toJson().encode("ASCII")

        if lmdbHasKey(self._db, _APRI_ID_KEY_PREFIX + new_apri_json):

            new_apri_id = self._getIdByApri(newApri, new_apri_json, False)
            new_id_apri_key = _ID_APRI_KEY_PREFIX + new_apri_id
            has_new_apri_already = True

            warnings.warn(f"This `Register` already has a reference to {str(newApri)}.")

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
                    with lmdbPrefixIter(ro_txn, _APRI_ID_KEY_PREFIX) as it:
                        for key, val in it:

                            if key == old_apri_id_key:
                                new_key = _APRI_ID_KEY_PREFIX + new_apri_json

                            else:

                                apri = ApriInfo.fromJson(key[_APRI_ID_KEY_PREFIX_LEN:].decode("ASCII"))
                                replaced = apri.changeInfo(oldApri, newApri)
                                new_key = _APRI_ID_KEY_PREFIX + replaced.toJson().encode("ASCII")

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
                            "The set of `ApriInfo` that changed under the replacement `oldApri` -> `newApri` must be "
                            "disjoint from the set of `ApriInfo` that did not change."
                        )

                    # change all id_apri keys
                    with lmdbPrefixIter(ro_txn, _ID_APRI_KEY_PREFIX) as it:
                        for key, val in it:

                            new_key = key

                            if key == old_id_apri_key:
                                new_val = new_apri_json

                            else:

                                apri = ApriInfo.fromJson(val.decode("ASCII"))
                                replaced = apri.changeInfo(oldApri, newApri)
                                new_val = replaced.toJson().encode("ASCII")

                            if has_new_apri_already and key == new_id_apri_key:
                                new_key = old_id_apri_key

                            if key != new_key or val != new_val:
                                rw_txn.put(new_key, new_val)

                    if has_new_apri_already:

                        # change all blocks
                        for prefix in [_BLK_KEY_PREFIX, _COMPRESSED_KEY_PREFIX]:

                            with lmdbPrefixIter(ro_txn, prefix + new_apri_id + _KEY_SEP) as it:
                                for key, val in it:

                                    new_blk_key = prefix + old_apri_id + key[key.index(_KEY_SEP) : ]
                                    rw_txn.put(new_blk_key, val)

                    # change all apos vals
                    with lmdbPrefixIter(ro_txn, _APOS_KEY_PREFIX) as it:
                        for key, val in it:

                            apos = AposInfo.fromJson(val.decode("ASCII"))
                            replaced = apos.changeInfo(oldApri, newApri)
                            new_val = replaced.toJson().encode("ASCII")

                            if val != new_val:
                                rw_txn.put(new_key, new_val)

                    if _debug == 2:
                        raise KeyboardInterrupt

                if _debug == 3:
                    raise KeyboardInterrupt

        except lmdb.MapFullError as e:
            raise RegisterError(_MEMORY_FULL_ERROR_MESSAGE.format(self._dbMapSize)) from e

        if recursively:
            for subreg in self._iterSubregs():
                with subreg._recursiveOpen(False) as subreg:
                    subreg.changeApriInfo(oldApri, newApri, True)

    def rmvApriInfo(self, apri):
        """Remove an `ApriInfo` that is not associated with any other `ApriInfo`, `Block`, nor `AposInfo`.

        :param apri: (type `ApriInfo`)
        :raise ValueError: If there are any `ApriInfo`, `Block`, or `AposInfo` associated with `apri`.
        """

        # DEBUG : 1, 2, 3, 4

        self._checkOpenRaise("rmvApriInfo")

        self._checkReadwriteRaise("rmvApriInfo")

        if not isinstance(apri, ApriInfo):
            raise TypeError("`apri` must be of type `ApriInfo`.")

        _id = self._getIdByApri(apri, None, False)

        if self.numDiskBlks(apri) != 0:
            raise ValueError(
                f"There are disk `Block`s saved with `{str(apri)}`. Please remove them first and call "
                "`rmvApriInfo` again."
            )

        if _debug == 1:
            raise KeyboardInterrupt

        with lmdbPrefixIter(self._db, _ID_APRI_KEY_PREFIX) as it:

            for _, _apri_json in it:

                _apri = ApriInfo.fromJson(_apri_json.decode("ASCII"))

                if apri in _apri and apri != _apri:

                    raise ValueError(
                        f"{str(_apri)} is associated with {str(apri)}. Please remove the former first before removing "
                        "the latter."
                    )

        if _debug == 2:
            raise KeyboardInterrupt

        try:
            self.aposInfo(apri)

        except DataNotFoundError:
            pass

        else:
            raise ValueError(
                f"There is an `AposInfo` associated with `{str(apri)}`. Please remove it first and call "
                "`rmvApriInfo` again."
            )

        if _debug == 3:
            raise KeyboardInterrupt

        try:

            with self._db.begin(write = True) as txn:

                txn.delete(_ID_APRI_KEY_PREFIX + _id)
                txn.delete(_APRI_ID_KEY_PREFIX + apri.toJson().encode("ASCII"))

                if _debug == 4:
                    raise KeyboardInterrupt

        except lmdb.MapFullError as e:
            raise RegisterError(_MEMORY_FULL_ERROR_MESSAGE.format(self._dbMapSize)) from e

    #################################
    #      PROTEC APRI METHODS      #

    def _getApriJsonById(self, _id, txn = None):
        """Get JSON bytestring representing an `ApriInfo` instance.

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

    def _getIdByApri(self, apri, apriJson, missingOk, txn = None):
        """Get an `ApriInfo` ID for this database. If `missingOk is True`, then create an ID if the passed `apri` or
        `apriJson` is unknown to this `Register`.

        One of `apri` and `apriJson` can be `None`, but not both. If both are not `None`, then `apri` is used.

        `self._db` must be opened by the caller.

        :param apri: (type `ApriInfo`)
        :param apriJson: (type `bytes`)
        :param missingOk: (type `bool`) Create an ID if the passed `apri` or `apriJson` is unknown to this `Register`.
        :param txn: (type `lmbd.Transaction`, default `None`) The transaction to query. If `None`, then use open a new
        transaction and commit it after this method resolves.
        :raises ApriInfo_Not_Found_Error: If `apri` or `apriJson` is not known to this `Register` and `missingOk
        is False`.
        :return: (type `bytes`)
        """

        if apri is not None:
            key = _APRI_ID_KEY_PREFIX + apri.toJson().encode("ASCII")

        elif apriJson is not None:
            key = _APRI_ID_KEY_PREFIX + apriJson

        else:
            raise ValueError

        commit = txn is None

        if commit and missingOk:
            txn = self._db.begin(write = True)

        elif commit:
            txn = self._db.begin()

        try:
            _id = txn.get(key, default = None)

            if _id is not None:
                return _id

            elif missingOk:

                _id = txn.get(_CURR_ID_KEY)
                next_id = str(int(_id) + 1).encode("ASCII")

                txn.put(_CURR_ID_KEY, next_id)
                txn.put(key, _id)
                txn.put(_ID_APRI_KEY_PREFIX + _id, key[_APRI_ID_KEY_PREFIX_LEN : ])

                return _id

            else:

                if apri is None:
                    apri = ApriInfo.fromJson(apriJson.decode("ASCII"))

                raise DataNotFoundError(f"`{str(apri)}` is not known to this `Register`.")

        finally:

            if commit:

                try:
                    txn.commit()

                except lmdb.MapFullError as e:
                    raise RegisterError(_MEMORY_FULL_ERROR_MESSAGE.format(self._dbMapSize)) from e

    #################################
    #      PUBLIC APOS METHODS      #

    def setAposInfo(self, apri, apos):
        """Set some `AposInfo` for corresponding `ApriInfo`.

        WARNING: This method will OVERWRITE any previous saved `AposInfo`. If you do not want to lose any previously
        saved data, then you should do something like the following:

            apos = reg.aposInfo(apri)
            apos.period_length = 5
            reg.setAposInfo(apos)

        :param apri: (type `ApriInfo`)
        :param apos: (type `AposInfo`)
        """

        # DEBUG : 1, 2

        self._checkOpenRaise("setAposInfo")

        self._checkReadwriteRaise("setAposInfo")

        if not isinstance(apri, ApriInfo):
            raise TypeError("`apri` must be of type `ApriInfo`")

        if not isinstance(apos, AposInfo):
            raise TypeError("`apos` must be of type `AposInfo`")

        key = self._getAposKey(apri, None, True)
        apos_json = apos.toJson().encode("ASCII")

        if _debug == 1:
            raise KeyboardInterrupt

        try:

            with self._db.begin(write = True) as txn:

                txn.put(key, apos_json)

                if _debug == 2:
                    raise KeyboardInterrupt

        except lmdb.MapFullError as e:
            raise RegisterError(_MEMORY_FULL_ERROR_MESSAGE.format(self._dbMapSize)) from e

    def aposInfo(self, apri):
        """Get some `AposInfo` associated with a given `ApriInfo`.

        :param apri: (type `ApriInfo`)
        :raises DataNotFoundError: If no `AposInfo` has been associated to `apri`.
        :return: (type `AposInfo`)
        """

        self._checkOpenRaise("aposInfo")

        if not isinstance(apri, ApriInfo):
            raise TypeError("`apri` must be of type `ApriInfo`")

        key = self._getAposKey(apri, None, False)

        with self._db.begin() as txn:
            apos_json = txn.get(key, default=None)

        if apos_json is not None:
            return AposInfo.fromJson(apos_json.decode("ASCII"))

        else:
            raise DataNotFoundError(f"No `AposInfo` associated with `{str(apri)}`.")

    def rmvAposInfo(self, apri):

        # DEBUG : 1, 2

        self._checkOpenRaise("rmvAposInfo")

        self._checkReadwriteRaise("rmvAposInfo")

        if not isinstance(apri, ApriInfo):
            raise TypeError("`apri` must be of type `ApriInfo`.")

        key = self._getAposKey(apri, None, False)

        if _debug == 1:
            raise KeyboardInterrupt

        if lmdbHasKey(self._db, key):

            try:

                with self._db.begin(write = True) as txn:

                    txn.delete(key)

                    if _debug == 2:
                        raise KeyboardInterrupt

            except lmdb.MapFullError as e:
                raise RegisterError(_MEMORY_FULL_ERROR_MESSAGE.format(self._dbMapSize)) from e

        else:
            raise DataNotFoundError(f"No `AposInfo` associated with `{str(apri)}`.")

    #################################
    #      PROTEC APOS METHODS      #

    def _getAposKey(self, apri, apriJson, missingOk, txn = None):
        """Get a key for an `AposInfo` entry.

        One of `apri` and `apriJson` can be `None`, but not both. If both are not `None`, then `apri` is used. If
        `missingOk is True`, then create a new `ApriInfo` ID if one does not already exist for `apri`.

        :param apri: (type `ApriInfo`)
        :param apriJson: (type `bytes`)
        :param missingOk: (type `bool`)
        :param txn: (type `lmbd.Transaction`, default `None`) The transaction to query. If `None`, then use open a new
        transaction and commit it after this method resolves.
        :raises DataNotFoundError: If `missingOk is False` and `apri` is not known to this `Register`.
        :return: (type `bytes`)
        """

        if apri is None and apriJson is None:
            raise ValueError

        apri_id = self._getIdByApri(apri, apriJson, missingOk, txn)

        return _APOS_KEY_PREFIX + _KEY_SEP + apri_id

    #################################
    #  PUBLIC SUB-REGISTER METHODS  #

    def addSubreg(self, subreg):

        # DEBUG : 1, 2

        self._checkOpenRaise("addSubreg")

        self._checkReadwriteRaise("addSubreg")

        if not isinstance(subreg, Register):
            raise TypeError("`subreg` must be of a `Register` derived type")

        if not subreg._created:
            raise RegisterError(_NOT_CREATED_ERROR_MESSAGE.format("addSubreg"))

        key = subreg._getSubregKey()

        if _debug == 1:
            raise KeyboardInterrupt

        if not lmdbHasKey(self._db, key):

            if subreg._checkNoCyclesFrom(self):

                try:

                    with self._db.begin(write = True) as txn:

                        txn.put(key, _SUB_VAL)

                        if _debug == 2:
                            raise KeyboardInterrupt

                except lmdb.MapFullError as e:
                    raise RegisterError(_MEMORY_FULL_ERROR_MESSAGE.format(self._dbMapSize)) from e

            else:

                raise RegisterError(
                    "Attempting to add this register as a sub-register will created a directed cycle in the " +
                    "subregister relation. "
                    f'Intended super-register description: "{str(self)}". '
                    f'Intended sub-register description: "{str(subreg)}".'
                )

        else:
            raise RegisterError("`Register` already added as subregister.")

    def rmvSubreg(self, subreg):
        """
        :param subreg: (type `Register`)
        """

        # DEBUG : 1, 2

        self._checkOpenRaise("rmvSubreg")

        self._checkReadwriteRaise("rmvSubreg")

        if not isinstance(subreg, Register):
            raise TypeError("`subreg` must be of a `Register` derived type.")

        key = subreg._getSubregKey()

        if _debug == 1:
            raise KeyboardInterrupt

        if lmdbHasKey(self._db, key):

            try:

                with self._db.begin(write = True) as txn:

                    txn.delete(key)

                    if _debug == 2:
                        raise KeyboardInterrupt

            except lmdb.MapFullError as e:
                raise RegisterError(_MEMORY_FULL_ERROR_MESSAGE.format(self._dbMapSize)) from e


        else:
            raise RegisterError("`Register` not added as subregister.")

    #################################
    #  PROTEC SUB-REGISTER METHODS  #

    def _checkNoCyclesFrom(self, original, touched = None):
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
            raise RegisterError(_NOT_CREATED_ERROR_MESSAGE.format("_checkNoCyclesFrom"))

        if self is original:
            return False

        if touched is None:
            touched = set()

        with self._recursiveOpen(True) as reg:

            if any(
                original is subreg
                for subreg in reg._iterSubregs()
            ):
                return False

            for subreg in reg._iterSubregs():

                if subreg not in touched:

                    touched.add(subreg)
                    if not subreg._checkNoCyclesFrom(original, touched):
                        return False


            else:
                return True

    def _iterSubregs(self):

        with lmdbPrefixIter(self._db, _SUB_KEY_PREFIX) as it:

            for key, _ in it:

                localDir = Path(key[_SUB_KEY_PREFIX_LEN : ].decode("ASCII"))
                subreg = Register._fromLocalDir(localDir)
                yield subreg

    def _getSubregKey(self):
        return _SUB_KEY_PREFIX + self._localDirBytes

    #################################
    #    PUBLIC DISK BLK METHODS    #

    @classmethod
    @abstractmethod
    def dumpDiskData(cls, data, filename, **kwargs):
        """Dump data to the disk.

        This method should not change any properties of any `Register`, which is why it is a class-method and
        not an instance-method. It merely takes `data`, processes it, and dumps it to disk.

        Most use-cases prefer the instance-method `addDiskBlk`.

        :param data: (any type) The raw data to dump.
        :param filename: (type `pathlib.Path`) The filename to dump to. You may edit this filename if
        necessary (such as by adding a suffix), but you must return the edited filename.
        :return: (type `pathlib.Path`) The actual filename of the data on the disk.
        """

    @classmethod
    @abstractmethod
    def loadDiskData(cls, filename, **kwargs):
        """Load raw data from the disk.

        This method should not change any properties of any `Register`, which is why it is a classmethod and
        not an instancemethod. It merely loads the data saved on the disk, processes it, and returns it.

        Most use-cases prefer the method `diskBlk`.

        :param filename: (type `pathlib.Path`) Where to load the block from. You may need to edit this
        filename if necessary, such as by adding a suffix, but you must return the edited filename.
        :raises DataNotFoundError: If the data could not be loaded because it doesn't exist.
        :return: (any type) The data loaded from the disk.
        :return: (pathlib.Path) The exact path of the data saved to the disk.
        """

    @classmethod
    def cleanDiskData(cls, filename, **kwargs):
        """Remove raw data from the disk.

        This method should not change any properties of any `Register`, which is why it is a classmethod and
        not an instancemethod. It merely removes the raw data.

        Most use-cases prefer the method `rmvDiskBlk`.

        :param filename: (type `pathlib.Path`) Where to remove the raw data. You may need to edit this
        filename if necessary, such as by adding a suffix, but you must return the edited filename.
        :raises DataNotFoundError: If the data could not be cleaned because it doesn't exist.
        :return: (pathlib.Path) The exact path of the data removed.
        """

        if not filename.is_absolute():
            raise ValueError(NOT_ABSOLUTE_ERROR_MESSAGE.format(filename))

        try:
            filename.unlink(missing_ok = False)

        except FileNotFoundError as e:
            raise DataNotFoundError(f"No file found at {str(filename)}") from e

        return filename

    def addDiskBlk(self, blk, retMetadata = False, **kwargs):
        """Save a `Block` to disk and link it with this `Register`.

        :param blk: (type `Block`)
        :param retMetadata: (type `bool`, default `False`) Whether to return a `File_Metadata` object, which
        contains file creation date/time and size of dumped data to the disk.
        :raises RegisterError: If a duplicate `Block` already exists in this `Register`.
        """

        #DEBUG : 1, 2, 3, 4

        _FAIL_NO_RECOVER_ERROR_MESSAGE = "Could not successfully recover from a failed disk `Block` add!"

        self._checkOpenRaise("addDiskBlk")

        self._checkReadwriteRaise("addDiskBlk")

        if not isinstance(blk, Block):
            raise TypeError("`blk` must be of type `Block`.")

        if not isinstance(retMetadata, bool):
            raise TypeError("`retMetadata` must be of type `bool`.")

        startn_head = blk.startn() // self._startnTailMod

        if startn_head != self._startnHead :

            raise IndexError(
                "The `startn` for the passed `Block` does not have the correct head:\n" +
                f"`tailLen`   : {self._startnTailLength}\n" +
                f"expected `head` : {self._startnHead}\n"
                f"`startn`       : {blk.startn()}\n" +
                f"`startn` head  : {startn_head}\n" +
                "Please see the method `setstartnInfo` to troubleshoot this error."
            )

        apris = [apri for _, apri in blk.apri().iterInnerInfo() if isinstance(apri, ApriInfo)]

        filename = None

        if _debug == 1:
            raise KeyboardInterrupt

        try:

            with self._db.begin(write = True) as rw_txn:

                with self._db.begin() as ro_txn:

                    # this will create ID's if necessary
                    for i, apri in enumerate(apris):
                        self._getIdByApri(apri, None, True, rw_txn)

                    blk_key = self._getDiskBlockKey(

                        _BLK_KEY_PREFIX,
                        blk.apri(), None, blk.startn(), len(blk),
                        False, rw_txn
                    )

                    if not lmdbHasKey(ro_txn, blk_key):

                        filename = randomUniqueFilename(self._localDir, length=6)

                        if _debug == 2:
                            raise KeyboardInterrupt

                        filename = type(self).dumpDiskData(blk.segment(), filename, **kwargs)

                        if _debug == 3:
                            raise KeyboardInterrupt

                        filename_bytes = str(filename.name).encode("ASCII")
                        compressed_key = _COMPRESSED_KEY_PREFIX + blk_key[_BLK_KEY_PREFIX_LEN : ]

                        rw_txn.put(blk_key, filename_bytes)
                        rw_txn.put(compressed_key, _IS_NOT_COMPRESSED_VAL)

                        if len(blk) == 0:

                            warnings.warn(
                                "Added a length 0 disk `Block` to this `Register`.\n" +
                                f"`Register` msg: {str(self)}\n" +
                                f"`Block`: {str(blk)}\n" +
                                f"`Register` location: {str(self._localDir)}"
                            )

                        if retMetadata:
                            return FileMetadata.fromPath(filename)

                    else:

                        raise RegisterError(
                            f"Duplicate `Block` with the following data already exists in this `Register`: " +
                            f"{str(blk.apri())}, startn = {blk.startn()}, length = {len(blk)}."
                        )

                if _debug == 4:
                    raise KeyboardInterrupt

        except BaseException as e:
            # We must assume that if an exception was thrown, `rw_txn` did not commit successfully.

            try:

                if filename is not None:
                    filename.unlink(missing_ok = True)

            except BaseException:
                raise RegisterRecoveryError(_FAIL_NO_RECOVER_ERROR_MESSAGE)

            else:

                if isinstance(e, lmdb.MapFullError):
                    raise RegisterError(_MEMORY_FULL_ERROR_MESSAGE.format(self._dbMapSize)) from e

                else:
                    raise e

    def appendDiskBlk(self, blk, retMetadata = False, **kwargs):
        """Add a `Block` to disk and link it with this `Register`.

        If no disk `Block`s with the same `ApriInfo` as `blk` have previously been added to disk, then the `startn`
        of `blk` will be set to 0. If not, then `startn` will be set to one more than the largest index among all
        disk `Block`s with the same `ApriInfo` as `blk`.

        :param blk: (type `Block`)
        :param retMetadata: (type `bool`, default `False`) Whether to return a `File_Metadata` object, which
        contains file creation date/time and size of dumped data to the disk.
        :raises RegisterError: If a duplicate `Block` already exists in this `Register`.
        """

        if not isinstance(blk, Block):
            raise TypeError("`blk` must be of type `Block`.")

        if not isinstance(retMetadata, bool):
            raise TypeError("`retMetadata` must be of type `bool`.")

        if self.numDiskBlks(blk.apri()) == 0:

            blk.setstartn(0)
            self.addDiskBlk(blk, **kwargs)

        else:

            startn = 0

            for key, _ in self._iterDiskBlockPairs(_BLK_KEY_PREFIX, blk.apri(), None):

                _, _startn, _length = self._convertDiskBlockKey(_BLK_KEY_PREFIX_LEN, key)

                if startn < _startn + _length:
                    startn = _startn + _length

            blk.setstartn(startn)
            self.addDiskBlk(blk, **kwargs)

    def rmvDiskBlk(self, apri, startn = None, length = None, recursively = False, **kwargs):
        """Delete a disk `Block` and unlink it with this `Register`.

        :param apri: (type `ApriInfo`)
        :param startn: (type `int`) Non-negative.
        :param length: (type `int`) Non-negative.
        :param recursively: (type `bool`)
        """

        # DEBUG : 1, 2, 3

        _FAIL_NO_RECOVER_ERROR_MESSAGE = "Could not successfully recover from a failed disk `Block` remove!"

        self._checkOpenRaise("rmvDiskBlk")

        self._checkReadwriteRaise("rmvDiskBlk")

        startn, length = Register._checkApristartnLengthRaise(apri, startn, length)

        startn, length = self._resolvestartnLength(apri, startn, length)

        try:

            blk_key, compressed_key = self._checkBlkCompressedKeysRaise(None, None, apri, None, startn, length)

            if _debug == 1:
                raise KeyboardInterrupt

        except DataNotFoundError:
            pass

        else:

            blk_filename, compressed_filename = self._checkBlkCompressedFilesRaise(
                blk_key, compressed_key, apri, startn, length
            )

            if not isDeletable(blk_filename):
                raise OSError(f"Cannot delete `Block` file `{str(blk_filename)}`.")

            if compressed_filename is not None and not isDeletable(compressed_filename):
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
                    type(self).cleanDiskData(blk_filename, **kwargs)

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
                                raise RegisterRecoveryError(_FAIL_NO_RECOVER_ERROR_MESSAGE)

                        else:

                            if blk_filename.exists():

                                with self._db.begin(write = True) as txn:

                                    txn.put(compressed_key, compressed_val)
                                    txn.put(blk_key, blk_val)

                            else:
                                raise RegisterRecoveryError(_FAIL_NO_RECOVER_ERROR_MESSAGE)

                    except RegisterRecoveryError as ee:
                        raise ee

                    except BaseException:
                        raise RegisterRecoveryError(_FAIL_NO_RECOVER_ERROR_MESSAGE)

                if isinstance(e, lmdb.MapFullError):
                    raise RegisterError(_MEMORY_FULL_ERROR_MESSAGE.format(self._dbMapSize)) from e

                else:
                    raise e

            return

        if recursively:

            for subreg in self._iterSubregs():

                with subreg._recursiveOpen(False) as subreg:

                    try:
                        subreg.rmvDiskBlk(apri, startn, length, True, **kwargs)

                    except DataNotFoundError:
                        pass

                    else:
                        return

        raise DataNotFoundError(
            _DISK_BLOCK_DATA_NOT_FOUND_ERROR_MSG_FULL.format(str(apri), startn, length)
        )

    def diskBlk(self, apri, startn = None, length = None, retMetadata = False, recursively = False, **kwargs):

        self._checkOpenRaise("diskBlk")

        startn, length = Register._checkApristartnLengthRaise(apri, startn, length)

        if not isinstance(retMetadata, bool):
            raise TypeError("`retMetadata` must be of type `bool`.")

        if not isinstance(recursively, bool):
            raise TypeError("`recursively` must be of type `bool`.")

        startn, length = self._resolvestartnLength(apri, startn, length)

        try:
            blk_key, compressed_key = self._checkBlkCompressedKeysRaise(None, None, apri, None, startn, length)

        except DataNotFoundError:
            pass

        else:

            with self._db.begin() as txn:
                if txn.get(compressed_key) != _IS_NOT_COMPRESSED_VAL:
                    raise CompressionError(
                        "Could not load `Block` with the following data because the `Block` is compressed. Please call " +
                        "the `Register` method `decompress` first before loading the data.\n" +
                        f"{apri}, startn = {startn}, length = {length}"
                    )

            blk_filename, _ = self._checkBlkCompressedFilesRaise(blk_key, compressed_key, apri, startn, length)
            blk_filename = self._localDir / blk_filename
            data, blk_filename = type(self).loadDiskData(blk_filename, **kwargs)
            blk = Block(data, apri, startn)

            if retMetadata:
                return blk, FileMetadata.fromPath(blk_filename)

            else:
                return blk

        if recursively:
            for subreg in self._iterSubregs():
                with subreg._recursiveOpen(True) as subreg:
                    try:
                        return subreg.diskBlk(apri, startn, length, retMetadata, True)

                    except DataNotFoundError:
                        pass

        raise DataNotFoundError(
            _DISK_BLOCK_DATA_NOT_FOUND_ERROR_MSG_FULL.format(str(apri), startn, length)
        )

    def diskBlkByN(self, apri, n, retMetadata = False, recursively = False, **kwargs):

        self._checkOpenRaise("diskBlkByN")

        if not isinstance(apri, ApriInfo):
            raise TypeError("`apri` must be of type `ApriInfo`.")

        if not isInt(n):
            raise TypeError("`n` must be of type `int`.")
        else:
            n = int(n)

        if not isinstance(retMetadata, bool):
            raise TypeError("`retMetadata` must be of type `bool`.")

        if not isinstance(recursively, bool):
            raise TypeError("`recursively` must be of type `bool`.")

        if n < 0:
            raise ValueError("`n` must be non-negative")

        try:
            for startn, length in self.diskIntervals(apri):
                if startn <= n < startn + length:
                    return self.diskBlk(apri, startn, length, retMetadata, False, **kwargs)

        except DataNotFoundError:
            pass

        if recursively:
            for subreg in self._iterSubregs():
                with subreg._recursiveOpen(True) as subreg:
                    try:
                        return subreg.diskBlkByN(apri, n, retMetadata, True, **kwargs)

                    except DataNotFoundError:
                        pass

        raise DataNotFoundError(_DISK_BLOCK_DATA_NOT_FOUND_ERROR_MSG_N.format(str(apri), n))

    def diskBlks(self, apri, retMetadata = False, recursively = False, **kwargs):

        self._checkOpenRaise("diskBlks")

        if not isinstance(apri, ApriInfo):
            raise TypeError("`apri` must be of type `ApriInfo`.")

        if not isinstance(retMetadata, bool):
            raise TypeError("`retMetadata` must be of type `bool`.")

        if not isinstance(recursively, bool):
            raise TypeError("`recursively` must be of type `bool`.")


        for startn, length in self.diskIntervals(apri):
            try:
                yield self.diskBlk(apri, startn, length, retMetadata, False, **kwargs)

            except DataNotFoundError:
                pass

        if recursively:
            for subreg in self._iterSubregs():
                with subreg._recursiveOpen(True) as subreg:
                    for blk in subreg.diskBlks(apri, retMetadata, True, **kwargs):
                        yield blk

    def diskBlkMetadata(self, apri, startn = None, length = None, recursively = False):

        self._checkOpenRaise("diskBlkMetadata")

        startn, length = Register._checkApristartnLengthRaise(apri, startn, length)

        if not isinstance(recursively, bool):
            raise TypeError("`recursively` must be of type `bool`.")

        startn, length = self._resolvestartnLength(apri, startn, length)

        try:
            blk_key, compressed_key = self._checkBlkCompressedKeysRaise(None, None, apri, None, startn, length)

        except DataNotFoundError:
            pass

        else:
            blk_filename, compressed_filename = self._checkBlkCompressedFilesRaise(
                blk_key, compressed_key, apri, startn, length
            )

            if compressed_filename is not None:
                return FileMetadata.fromPath(compressed_filename)

            else:
                return FileMetadata.fromPath(blk_filename)

        if recursively:
            for subreg in self._iterSubregs():
                with subreg._recursiveOpen(True) as subreg:
                    try:
                        return subreg.diskBlkMetadata(apri, startn, length, True)

                    except DataNotFoundError:
                        pass

        raise DataNotFoundError(
            _DISK_BLOCK_DATA_NOT_FOUND_ERROR_MSG_FULL.format(str(apri), startn, length)
        )

    def diskIntervals(self, apri):
        """Return a `list` of all tuples `(startn, length)` associated to disk `Block`s.

        The tuples are sorted by increasing `startn` and the larger `length` is used to break ties.

        :param apri: (type `ApriInfo`)
        :return: (type `list`)
        """

        self._checkOpenRaise("diskIntervals")

        if not isinstance(apri, ApriInfo):
            raise TypeError("`apri` must be of type `ApriInfo`.")

        return sorted([
            self._convertDiskBlockKey(_BLK_KEY_PREFIX_LEN, key, apri)[1:]
            for key, _ in self._iterDiskBlockPairs(_BLK_KEY_PREFIX, apri, None)
        ], key = lambda t: (t[0], -t[1]))

    def numDiskBlks(self, apri):

        self._checkOpenRaise("numDiskBlks")

        try:

            return lmdbCountKeys(
                self._db,
                _BLK_KEY_PREFIX + self._getIdByApri(apri, None, False) + _KEY_SEP
            )

        except DataNotFoundError:
            return 0

    def compress(self, apri, startn = None, length = None, compressionLevel = 6, retMetadata = False):
        """Compress a `Block`.

        :param apri: (type `ApriInfo`)
        :param startn: (type `int`) Non-negative.
        :param length: (type `int`) Non-negative.
        :param compressionLevel: (type `int`, default 6) Between 0 and 9, inclusive. 0 is for the fastest compression,
        but lowest compression ratio; 9 is slowest, but highest ratio. See
        https://docs.python.org/3/library/zlib.html#zlib.compressobj for more information.
        :param retMetadata: (type `bool`, default `False`) Whether to return a `File_Metadata` object that
        describes the compressed file.
        :raises CompressionError: If the `Block` is already compressed.
        :return: (type `File_Metadata`) If `retMetadata is True`.
        """

        # DEBUG : 1, 2, 3, 4

        _FAIL_NO_RECOVER_ERROR_MESSAGE = "Could not recover successfully from a failed disk `Block` compress!"

        self._checkOpenRaise("compress")

        self._checkReadwriteRaise("compress")

        startn, length = Register._checkApristartnLengthRaise(apri, startn, length)

        if not isInt(compressionLevel):
            raise TypeError("`compressionLevel` must be of type `int`.")
        else:
            compressionLevel = int(compressionLevel)

        if not isinstance(retMetadata, bool):
            raise TypeError("`retMetadata` must be of type `bool`.")

        if not (0 <= compressionLevel <= 9):
            raise ValueError("`compressionLevel` must be between 0 and 9.")

        startn, length = self._resolvestartnLength(apri, startn, length)

        compressed_key = self._getDiskBlockKey(
            _COMPRESSED_KEY_PREFIX, apri, None, startn, length, False
        )

        blk_key, compressed_key = self._checkBlkCompressedKeysRaise(
            None, compressed_key, apri, None, startn, length
        )

        with self._db.begin() as txn:
            compressed_val = txn.get(compressed_key)

        if compressed_val != _IS_NOT_COMPRESSED_VAL:

            raise CompressionError(
                "The disk `Block` with the following data has already been compressed: " +
                f"{str(apri)}, startn = {startn}, length = {length}"
            )

        with self._db.begin() as txn:
            blk_filename = self._localDir / txn.get(blk_key).decode("ASCII")

        compressed_filename = randomUniqueFilename(self._localDir, COMPRESSED_FILE_SUFFIX)
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
                compressionLevel,
                strict_timestamps=False  # change timestamps of old or new files

            ) as compressed_fh:

                compressed_fh.write(blk_filename, blk_filename.name)

                if _debug == 3:
                    raise KeyboardInterrupt

            if _debug == 4:
                raise KeyboardInterrupt

            type(self).cleanDiskData(blk_filename)
            cleaned = True
            blk_filename.touch(exist_ok = False)

        except BaseException as e:

            try:

                with self._db.begin(write = True) as txn:
                    txn.put(compressed_key, _IS_NOT_COMPRESSED_VAL)

                if cleaned or not blk_filename.exists():
                    raise RegisterRecoveryError(_FAIL_NO_RECOVER_ERROR_MESSAGE)

                else:
                    compressed_filename.unlink(missing_ok = True)

            except RegisterRecoveryError as ee:
                raise ee

            except BaseException:
                raise RegisterRecoveryError(_FAIL_NO_RECOVER_ERROR_MESSAGE)

            else:

                if isinstance(e, lmdb.MapFullError):
                    raise RegisterError(_MEMORY_FULL_ERROR_MESSAGE.format(self._dbMapSize)) from e

                else:
                    raise e

        if retMetadata:
            return FileMetadata.fromPath(compressed_filename)

        else:
            return None

    def decompress(self, apri, startn = None, length = None, retMetadata = False):
        """Decompress a `Block`.

        :param apri: (type `ApriInfo`)
        :param startn: (type `int`) Non-negative.
        :param length: (type `int`) Non-negative.
        :param retMetadata: (type `bool`, default `False`) Whether to return a `File_Metadata` object that
        describes the decompressed file.
        :raise DecompressionError: If the `Block` is not compressed.
        :return: (type `list`) If `retMetadata is True`.
        """

        # DEBUG : 1, 2, 3, 4

        _FAIL_NO_RECOVER_ERROR_MESSAGE = "Could not recover successfully from a failed disk `Block` decompress!"

        self._checkOpenRaise("decompress")

        self._checkReadwriteRaise("decompress")

        startn, length = Register._checkApristartnLengthRaise(apri, startn, length)

        if not isinstance(retMetadata, bool):
            raise TypeError("`retMetadata` must be of type `bool`.")

        startn, length = self._resolvestartnLength(apri, startn, length)

        blk_key, compressed_key = self._checkBlkCompressedKeysRaise(None, None, apri, None, startn, length)

        with self._db.begin() as txn:
            compressed_val = txn.get(compressed_key)

        if compressed_val == _IS_NOT_COMPRESSED_VAL:

            raise DecompressionError(
                "The disk `Block` with the following data is not compressed: " +
                f"{str(apri)}, startn = {startn}, length = {length}"
            )

        with self._db.begin() as txn:
            blk_filename = txn.get(blk_key).decode("ASCII")

        blk_filename = self._localDir / blk_filename
        compressed_filename = self._localDir / compressed_val.decode("ASCII")
        deleted = False

        if not isDeletable(blk_filename):
            raise OSError(f"Cannot delete ghost file `{str(blk_filename)}`.")

        if not isDeletable(compressed_filename):
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

                    compressed_fh.extract(blk_filename.name, self._localDir)

                    if _debug == 3:
                        raise KeyboardInterrupt

                txn.put(compressed_key, _IS_NOT_COMPRESSED_VAL)

                if _debug == 4:
                    raise KeyboardInterrupt

                compressed_filename.unlink(missing_ok = False)

        except BaseException as e:

            try:

                if not compressed_filename.is_file():
                    raise RegisterRecoveryError(_FAIL_NO_RECOVER_ERROR_MESSAGE)

                elif deleted or not blk_filename.is_file():

                    blk_filename.unlink(missing_ok = True)
                    blk_filename.touch(exist_ok = False)

            except RegisterRecoveryError as ee:
                raise ee

            except BaseException:
                raise RegisterRecoveryError(_FAIL_NO_RECOVER_ERROR_MESSAGE)

            else:

                if isinstance(e, lmdb.MapFullError):
                    raise RegisterError(_MEMORY_FULL_ERROR_MESSAGE.format(self._dbMapSize)) from e

                else:
                    raise e

        if retMetadata:
            return FileMetadata.fromPath(blk_filename)

        else:
            return None

    def setHashing(self, apri, hashing):
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

    def _getDiskBlockKey(self, prefix, apri, apriJson, startn, length, missingOk, txn = None):
        """Get the database key for a disk `Block`.

        One of `apri` and `apriJson` can be `None`, but not both. If both are not `None`, then `apri` is used.
        `self._db` must be opened by the caller. This method only queries the database to obtain the `apri` ID.

        If `missingOk is True` and an ID for `apri` does not already exist, then a new one will be created. If
        `missingOk is False` and an ID does not already exist, then an error is raised.

        :param prefix: (type `bytes`)
        :param apri: (type `ApriInfo`)
        :param apriJson: (types `bytes`)
        :param startn: (type `int`) The start index of the `Block`.
        :param length: (type `int`) The length of the `Block`.
        :param missingOk: (type `bool`)
        :param txn: (type `lmbd.Transaction`, default `None`) The transaction to query. If `None`, then use open a new
        transaction and commit it after this method resolves.
        :raises DataNotFoundError: If `missingOk is False` and `apri` is not known to this `Register`.
        :return: (type `bytes`)
        """

        if apri is None and apriJson is None:
            raise ValueError

        _id = self._getIdByApri(apri, apriJson, missingOk, txn)
        tail = startn % self._startnTailMod

        return (
                prefix                      +
                _id                         + _KEY_SEP +
                str(tail)  .encode("ASCII") + _KEY_SEP +
                str(length).encode("ASCII")
        )

    def _iterDiskBlockPairs(self, prefix, apri, apriJson, txn = None):
        """Iterate over key-value pairs for block entries.

        :param prefix: (type `bytes`)
        :param apri: (type `ApriInfo`)
        :param apriJson: (type `bytes`)
        :param txn: (type `lmbd.Transaction`, default `None`) The transaction to query. If `None`, then use open a new
        transaction and commit it after this method resolves.
        :raise DataNotFoundError: If `apri` is not a disk `ApriInfo`.
        :return: (type `bytes`) key
        :return: (type `bytes`) val
        """


        if apriJson is not None or apri is not None:

            prefix += self._getIdByApri(apri, apriJson, False, txn)
            prefix += _KEY_SEP

        if txn is None:
            txn = self._db

        with lmdbPrefixIter(txn, prefix) as it:
            for key,val in it:
                yield key, val

    @staticmethod
    def _splitDiskBlockKey(prefixLen, key):
        return tuple(key[prefixLen:].split(_KEY_SEP))

    @staticmethod
    def _joinDiskBlockData(prefix, apriJson, startnBytes, lenBytes):
        return (
                prefix +
                apriJson + _KEY_SEP +
                startnBytes + _KEY_SEP +
                lenBytes
        )

    def _convertDiskBlockKey(self, prefixLen, key, apri = None, txn = None):
        """
        :param prefixLen: (type `int`) Positive.
        :param key: (type `bytes`)
        :param apri: (type `ApriInfo`, default None) If `None`, the relevant `apri` is acquired through a database
        query.
        :param txn: (type `lmbd.Transaction`, default `None`) The transaction to query. If `None`, then use open a new
        transaction and commit it after this method resolves.
        :return: (type `ApriInfo`)
        :return (type `int`) startn
        :return (type `int`) length, non-negative
        """

        apri_id, startn_bytes, length_bytes = Register._splitDiskBlockKey(prefixLen, key)

        if apri is None:

            apri_json = self._getApriJsonById(apri_id, txn)
            apri = ApriInfo.fromJson(apri_json.decode("ASCII"))

        return (
            apri,
            int(startn_bytes.decode("ASCII")) + self._startnHead * self._startnTailMod,
            int(length_bytes.decode("ASCII"))
        )

    def _checkBlkCompressedKeysRaise(self, blkKey, compressedKey, apri, apriJson, startn, length):

        if compressedKey is None and blkKey is None:
            compressedKey = self._getDiskBlockKey(_COMPRESSED_KEY_PREFIX, apri, apriJson, startn, length, False)

        if blkKey is not None and compressedKey is None:
            compressedKey = _COMPRESSED_KEY_PREFIX + blkKey[_BLK_KEY_PREFIX_LEN:]

        elif compressedKey is not None and blkKey is None:
            blkKey = _BLK_KEY_PREFIX + compressedKey[_COMPRESSED_KEY_PREFIX_LEN:]

        if apri is None:
            apri = ApriInfo.fromJson(apriJson.decode("ASCII"))

        if not lmdbHasKey(self._db, blkKey) or not lmdbHasKey(self._db, compressedKey):
            raise DataNotFoundError(
                _DISK_BLOCK_DATA_NOT_FOUND_ERROR_MSG_FULL.format(apri, startn, length)
            )

        return blkKey, compressedKey

    def _checkBlkCompressedFilesRaise(self, blkKey, compressedKey, apri, startn, length):

        with self._db.begin() as txn:
            blk_val = txn.get(blkKey)
            compressed_val = txn.get(compressedKey)

        blk_filename = self._localDir / blk_val.decode("ASCII")

        if compressed_val != _IS_NOT_COMPRESSED_VAL:
            compressed_filename = self._localDir / compressed_val.decode("ASCII")

            if not compressed_filename.exists() or not blk_filename.exists():
                raise DataNotFoundError(
                    _DISK_BLOCK_DATA_NOT_FOUND_ERROR_MSG_FULL.format(str(apri), startn, length)
                )

            return blk_filename, compressed_filename

        else:

            if not blk_filename.exists():
                raise DataNotFoundError(
                    _DISK_BLOCK_DATA_NOT_FOUND_ERROR_MSG_FULL.format(str(apri), startn, length)
                )

            return blk_filename, None

    @staticmethod
    def _checkApristartnLengthRaise(apri, startn, length):

        if not isinstance(apri, ApriInfo):
            raise TypeError("`apri` must be of type `ApriInfo`")

        if not isInt(startn) and startn is not None:
            raise TypeError("startn` must be an `int`")

        elif startn is not None:
            startn = int(startn)

        if not isInt(length) and length is not None:
            raise TypeError("`length` must be an `int`")

        elif length is not None:
            length = int(length)

        if startn is not None and startn < 0:
            raise ValueError("`startn` must be non-negative")

        if length is not None and length < 0:
            raise ValueError("`length` must be non-negative")

        return startn, length

    def _resolvestartnLength(self, apri, startn, length):
        """
        :param apri: (type `ApriInfo`)
        :param startn: (type `int` or `NoneType`) Non-negative.
        :param length: (type `int` or `NoneType`) Positive.
        :raise DataNotFoundError
        :raise ValueError: If `startn is None and length is not None`.
        :return: (type `int`) Resolved `startn`, always `int`.
        :return: (type `int`) Resolved `length`, always `length`.
        """

        if startn is not None and length is not None:
            return startn, length

        elif startn is not None and length is None:

            key = self._getDiskBlockKey(_BLK_KEY_PREFIX, apri, None, startn, 1, False)

            first_key_sep_index = key.find(_KEY_SEP)
            second_key_sep_index = key.find(_KEY_SEP, first_key_sep_index + 1)

            prefix = key [ : second_key_sep_index + 1]

            i = -1
            largest_length = None
            key_with_largest_length = None
            with lmdbPrefixIter(self._db, prefix) as it:

                for i, (key, _) in enumerate(it):

                    length = int(Register._splitDiskBlockKey(_BLK_KEY_PREFIX_LEN, key)[2].decode("ASCII"))

                    if largest_length is None or length > largest_length:

                        largest_length = length
                        key_with_largest_length = key

            if i == -1:
                raise DataNotFoundError(f"No disk `Block`s found with {str(apri)} and startn = {startn}.")

            else:
                return self._convertDiskBlockKey(_BLK_KEY_PREFIX_LEN, key_with_largest_length, apri)[1:]

        elif startn is None and length is None:

            prefix = _BLK_KEY_PREFIX + self._getIdByApri(apri, None, False) + _KEY_SEP

            smallest_startn = None
            i = -1
            with lmdbPrefixIter(self._db, prefix) as it:

                for i, (key, _) in enumerate(it):

                    startn = int(Register._splitDiskBlockKey(_BLK_KEY_PREFIX_LEN, key)[1].decode("ASCII"))

                    if smallest_startn is None or startn < smallest_startn:
                        smallest_startn = startn

            if i == -1:
                raise DataNotFoundError(f"No disk `Block`s found with {str(apri)}.")

            else:
                return self._resolvestartnLength(apri, smallest_startn, None)

        else:
            raise ValueError(f"If you specify a `Block` length, you must also specify a `startn`.")

    #################################
    #    PUBLIC RAM BLK METHODS     #

    def addRamBlk(self, blk):

        if not isinstance(blk, Block):
            raise TypeError("`blk` must be of type `Block`.")

        if all(ram_blk is not blk for ram_blk in self._ramBlks):
            self._ramBlks.append(blk)

    def removeRamBlk(self, blk):

        if not isinstance(blk, Block):
            raise TypeError("`blk` must be of type `Block`.")

        for i, ram_blk in enumerate(self._ramBlks):

            if ram_blk is blk:

                del self._ramBlks[i]
                return

        raise DataNotFoundError(f"No RAM disk block found.")

    def ramBlkByN(self, apri, n, recursively = False):

        if not isinstance(apri, ApriInfo):
            raise TypeError("`apri` must be of type `ApriInfo`.")

        if not isInt(n):
            raise TypeError("`n` must be of type `int`.")
        else:
            n = int(n)

        if not isinstance(recursively, bool):
            raise TypeError("`recursively` must be of type `bool`.")

        if n < 0:
            raise IndexError("`n` must be non-negative")

        for blk in self._ramBlks:
            startn = blk.startn()
            if blk.apri() == apri and startn <= n < startn + len(blk):
                return blk

        if recursively:
            self._checkOpenRaise("ramBlkByN")
            for subreg in self._iterSubregs():
                with subreg._recursiveOpen(True) as subreg:
                    try:
                        return subreg.diskBlkByN(apri, n, True)
                    except DataNotFoundError:
                        pass

        raise DataNotFoundError(
            _RAM_BLOCK_DATA_NOT_FOUND_ERROR_MSG_N.format(str(apri), n)
        )

    def ramBlks(self, apri, recursively = False):

        if not isinstance(apri, ApriInfo):
            raise TypeError("`apri` must be of type `ApriInfo`.")

        if not isinstance(recursively, bool):
            raise TypeError("`recursively` must be of type `bool`.")

        for blk in self._ramBlks:
            if blk.apri() == apri:
                yield blk

        if recursively:
            self._checkOpenRaise("ramBlks")
            for subreg in self._iterSubregs():
                with subreg._recursiveOpen(True) as subreg:
                    for blk in subreg.ramBlks(apri, True):
                        yield blk

    #################################
    #    PROTEC RAM BLK METHODS     #

    #################################
    # PUBLIC RAM & DISK BLK METHODS #

    def __getitem__(self, apriNRecursively):

        if not isinstance(apriNRecursively, tuple) or len(apriNRecursively) <= 1:
            raise TypeError("Must pass at least two arguments to `reg[]`.")

        if len(apriNRecursively) >= 4:
            raise TypeError("Must pass at most three arguments to `reg[]`.")

        if len(apriNRecursively) == 2:

            apri, n = apriNRecursively
            recursively = False

        else:
            apri, n, recursively = apriNRecursively

        return self.get(apri, n, recursively)

    def get(self, apri, n, recursively = False, **kwargs):

        if not isinstance(apri, ApriInfo):
            raise TypeError("The first argument to `reg[]` must be an `ApriInfo.")

        if not isInt(n) and not isinstance(n, slice):
            raise TypeError("The second argument to `reg[]` must be an `int` or a `slice`.")

        elif isInt(n):
            n = int(n)

        else:

            _n = [None]*3

            if n.start is not None and not isInt(n.start):
                raise TypeError("Start index of slice must be an `int`.")

            elif n.start is not None:
                _n[0] = int(n.start)

            if n.stop is not None and not isInt(n.stop):
                raise TypeError("Stop index of slice must be an `int`.")

            elif n.stop is not None:
                _n[1] = int(n.stop)

            if n.step is not None and not isInt(n.step):
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
            return _ElementIter(self, apri, n, recursively, kwargs)

        else:

            try:
                return self.ramBlkByN(apri, n)[n]

            except DataNotFoundError:

                try:
                    return self.diskBlkByN(apri, n, **kwargs)[n]

                except DataNotFoundError:
                    pass

            raise DataNotFoundError(_DISK_RAM_BLOCK_DATA_NOT_FOUND_ERROR_MSG_N.format(str(apri), n))

    def intervals(self, apri, combine = True, recursively = False):

        if not isinstance(apri, ApriInfo):
            raise TypeError("`apri` must be of type `ApriInfo`.")

        if not isinstance(combine, bool):
            raise TypeError("`combine` must be of type `bool`.")

        if not isinstance(recursively, bool):
            raise TypeError("`recursively` must be of type `bool`.")

        intervals_sorted = sorted(
            [
                (startn, length)
                for _, startn, length in self._iterConvertedRamAndDiskBlockDatas(apri, recursively)
            ],
            key = lambda t: (t[0], -t[1])
        )

        if combine:

            intervals_reduced = []

            for int1 in intervals_sorted:
                for i, int2 in enumerate(intervals_reduced):
                    if intervalsOverlap(int1, int2):
                        a1, l1 = int1
                        a2, l2 = int2
                        if a2 + l2 < a1 + l1:
                            intervals_reduced[i] = (a2, a1 + l1 - a2)
                            break
                else:
                    intervals_reduced.append(int1)

            intervals_combined = []

            for startn, length in intervals_reduced:

                if len(intervals_combined) == 0 or intervals_combined[-1][0] + intervals_combined[-1][1] < startn:
                    intervals_combined.append((startn, length))

                else:
                    intervals_combined[-1] = (intervals_combined[-1][0], startn + length)

            return intervals_combined

        else:

            return intervals_sorted

    def totalLen(self, apri, recursively = False):

        self._checkOpenRaise("totalLen")

        if apri in self:
            return sum(length for _, length in self.intervals(apri, True, recursively))

        else:
            return 0

    #################################
    # PROTEC RAM & DISK BLK METHODS #

    def _iterConvertedRamAndDiskBlockDatas(self, apri, recursively = False):

        for blk in self._ramBlks:
            if blk.apri() == apri:
                yield apri, blk.startn(), len(blk)

        try:
            self._getIdByApri(apri, None, False)

        except DataNotFoundError:
            pass

        else:

            for key, _ in self._iterDiskBlockPairs(_BLK_KEY_PREFIX, apri, None):
                yield self._convertDiskBlockKey(_BLK_KEY_PREFIX_LEN, key, apri)

        if recursively:
            for subreg in self._iterSubregs():
                with subreg._recursiveOpen(True) as subreg:
                    for data in subreg._iter_ram_and_disk_block_datas(apri, True):
                        yield data

class PickleRegister(Register):

    @classmethod
    def dumpDiskData(cls, data, filename, **kwargs):

        if len(kwargs) > 0:
            raise KeyError("`Pickle_Register.addDiskBlk` accepts no keyword-arguments.")

        filename = filename.with_suffix(".pkl")

        with filename.open("wb") as fh:
            pickle.dump(data, fh)

        return filename

    @classmethod
    def loadDiskData(cls, filename, **kwargs):

        if len(kwargs) > 0:
            raise KeyError("`Pickle_Register.diskBlk` accepts no keyword-arguments.")

        with filename.open("rb") as fh:
            return pickle.load(fh), filename

Register.addSubclass(PickleRegister)

class NumpyRegister(Register):

    @classmethod
    def dumpDiskData(cls, data, filename, **kwargs):

        if len(kwargs) > 0:
            raise KeyError("This method accepts no keyword-arguments.")

        filename = filename.with_suffix(".npy")
        np.save(filename, data, allow_pickle = False, fix_imports = False)
        return filename

    @classmethod
    def loadDiskData(cls, filename, **kwargs):

        if "mmap_mode" in kwargs:
            mmap_mode = kwargs["mmap_mode"]

        else:
            mmap_mode = None

        if len(kwargs) > 1:
            raise KeyError("`Numpy_Register.get_disk_data` only accepts the keyword-argument `mmap_mode`.")

        if mmap_mode not in [None, "r+", "r", "w+", "c"]:
            raise ValueError(
                "The keyword-argument `mmap_mode` for `Numpy_Register.diskBlk` can only have the values " +
                "`None`, 'r+', 'r', 'w+', 'c'. Please see " +
                "https://numpy.org/doc/stable/reference/generated/numpy.memmap.html#numpy.memmap for more information."
            )

        return np.load(filename, mmap_mode = mmap_mode, allow_pickle = False, fix_imports = False), filename

    @classmethod
    def cleanDiskData(cls, filename, **kwargs):

        if len(kwargs) > 0:
            raise KeyError("This method accepts no keyword-arguments.")

        filename = filename.with_suffix(".npy")
        return Register.cleanDiskData(filename)

    def diskBlk(self, apri, startn = None, length = None, retMetadata = False, recursively = False, **kwargs):
        """
        :param apri: (type `ApriInfo`)
        :param startn: (type `int`)
        :param length: (type `length`) non-negative
        :param retMetadata: (type `bool`, default `False`) Whether to return a `File_Metadata` object, which
        contains file creation date/time and size of dumped saved on the disk.
        :param recursively: (type `bool`, default `False`) Search all subregisters for the `Block`.
        :param mmap_mode: (type `str`, optional) Load the Numpy file using memory mapping, see
        https://numpy.org/doc/stable/reference/generated/numpy.memmap.html#numpy.memmap for more information.
        :return: (type `File_Metadata`) If `retMetadata is True`.
        """

        ret = super().diskBlk(apri, startn, length, retMetadata, recursively, **kwargs)

        if retMetadata:
            blk = ret[0]

        else:
            blk = ret

        if isinstance(blk.segment(), np.memmap):
            blk = MemmapBlock(blk.segment(), blk.apri(), blk.startn())

        if retMetadata:
            return blk, ret[1]

        else:
            return blk

    def concatDiskBlks(self, apri, startn = None, length = None, delete = False, retMetadata = False):
        """Concatenate several `Block`s into a single `Block` along axis 0 and save the new one to the disk.

        If `delete = True`, then the smaller `Block`s are deleted automatically.

        The interval `range(startn, startn + length)` must be the disjoint union of intervals of the form
        `range(blk.startn(), blk.startn() + len(blk))`, where `blk` is a disk `Block` with `ApriInfo`
        given by `apri`.

        Length-0 `Block`s are ignored.

        If `startn` is not specified, it is taken to be the smallest `startn` of any `Block` saved to this
        `Register`. If `length` is not specified, it is taken to be the length of the largest
        contiguous set of indices that start with `startn`. If `startn` is not specified but `length` is, a
        ValueError is raised.

        :param apri: (type `ApriInfo`)
        :param startn: (type `int`) Non-negative.
        :param length: (type `int`) Positive.
        :param delete: (type `bool`, default `False`)
        :param retMetadata: (type `bool`, default `False`) Whether to return a `File_Metadata` object, which
        contains file creation date/time and size of dumped dumped to the disk.
        :raise DataNotFoundError: If the union of the intervals of relevant disk `Block`s does not equal
        `range(startn, startn + length)`.
        :raise ValueError: If any two intervals of relevant disk `Block`s intersect.
        :raise ValueError: If any two relevant disk `Block` segments have inequal shapes.
        :return: (type `File_Metadata`) If `retMetadata is True`.
        """

        _FAIL_NO_RECOVER_ERROR_MESSAGE = "Could not successfully recover from a failed disk `Block` concatenation!"

        self._checkOpenRaise("concatDiskBlks")

        self._checkReadwriteRaise("concatDiskBlks")

        startn, length = Register._checkApristartnLengthRaise(apri, startn, length)

        if not isinstance(retMetadata, bool):
            raise TypeError("`retMetadata` must be of type `bool`.")

        # infer startn
        startn, _ = self._resolvestartnLength(apri, startn, length)

        # this implementation depends on `diskIntervals` returning smaller startn before larger
        # ones and, when ties occur, larger lengths before smaller ones.

        if length is None:
            # infer length

            current_segment = False
            length = 0

            for _startn, _length in self.diskIntervals(apri):

                if _length > 0:

                    if current_segment:

                        if startn > _startn:
                            raise RuntimeError("Could not infer a value for `length`.")

                        elif startn == _startn:
                            raise ValueError(
                                f"Overlapping `Block` intervals found with {str(apri)}."
                            )

                        else:

                            if startn + length > _startn:
                                raise ValueError(
                                    f"Overlapping `Block` intervals found with {str(apri)}."
                                )

                            elif startn + length == _startn:
                                length += _length

                            else:
                                break

                    else:

                        if startn < _startn:
                            raise DataNotFoundError(
                                f"No disk `Block` found with the following data: {str(apri)}, startn = {startn}."
                            )

                        elif startn == _startn:

                            length += _length
                            current_segment = True

            if length == 0:
                raise RuntimeError("could not infer a value for `length`.")

            warnings.warn(f"`length` value not specified, inferred value: `length = {length}`.")

        combined_interval = None

        last_check = False
        last__startn = None

        intervals_to_get = []

        for _startn, _length in self.diskIntervals(apri):
            # infer blocks to combine

            if last_check:

                if last__startn == _startn and _length > 0:
                    raise ValueError(f"Overlapping `Block` intervals found with {str(apri)}.")

                else:
                    break

            if _length > 0:

                last__startn = _startn

                if _startn < startn:

                    if startn < _startn + _length:
                        raise ValueError(
                            f"The first `Block` is too long. Try again by calling `reg.concatDiskBlks({str(apri)}, " +
                            f"{_startn}, {length - (_startn - startn)})`."
                        )

                else:

                    if combined_interval is None:

                        if _startn > startn:

                            raise DataNotFoundError(
                                f"No disk `Block` found with the following data: `{str(apri)}, startn = {startn}`."
                            )

                        elif _startn == startn:

                            combined_interval = (_startn, _length)
                            intervals_to_get.append((_startn, _length))
                            last_check = _startn + _length == startn + length

                        else:
                            raise RuntimeError("Something went wrong trying to combine `Block`s.")

                    else:

                        if _startn > sum(combined_interval):

                            raise DataNotFoundError(
                                f"No `Block` found covering indices {sum(combined_interval)} through "
                                f"{_startn-1} (inclusive) with {str(apri)}."
                            )

                        elif _startn == sum(combined_interval):

                            if _startn + _length > startn + length:
                                raise ValueError(
                                    f"The last `Block` is too long. Try again by calling `reg.concatDiskBlks({str(apri)}, " +
                                    f"{startn}, {length - (_startn + _length - (startn + length))})`."
                                )

                            combined_interval = (startn, combined_interval[1] + _length)
                            intervals_to_get.append((_startn, _length))
                            last_check = _startn + _length == startn + length

                        else:
                            raise ValueError(f"Overlapping `Block` intervals found with {str(apri)}.")


        if len(intervals_to_get) == 1:

            if retMetadata:
                return self.diskBlkMetadata(apri, *intervals_to_get)

            else:
                return None

        blks = []
        fixed_shape = None
        ref_blk = None
        failure_reinsert_indices = []
        combined_blk = None

        try:

            for _startn, _length in intervals_to_get:
                # check that blocks have the correct shape

                blk = self.diskBlk(apri, _startn, _length, False, False, mmap_mode ="r")
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
                        f"{str(apri)}, startn = {_startn}, length = {_length}\n, shape = " +
                        f"{str(blk.segment().shape)}\n"

                    )

            combined_blk = np.concatenate([blk.segment() for blk in blks], axis=0)
            combined_blk = Block(combined_blk, apri, startn)
            ret = self.addDiskBlk(combined_blk, retMetadata)

            if _debug == 1:
                raise KeyboardInterrupt

            if delete:

                for blk in blks:

                    _startn = blk.startn()
                    _length = len(blk)
                    blk.close()
                    self.rmvDiskBlk(apri, _startn, _length, False)
                    failure_reinsert_indices.append((_startn, _length))

                    if _debug == 2:
                        raise KeyboardInterrupt

        except BaseException as e:

            try:

                if combined_blk is not None and isinstance(combined_blk, Block) and delete:

                    for _startn, _length in failure_reinsert_indices:
                        self.addDiskBlk(combined_blk[_startn: _startn + _length])

            except BaseException:
                raise RegisterRecoveryError(_FAIL_NO_RECOVER_ERROR_MESSAGE)

            else:
                raise e

        finally:

            for blk in blks:
                blk.close()

        return ret

Register.addSubclass(NumpyRegister)

class _ElementIter:

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

    def updateIntervalsCalculated(self):
        self.intervals = dict(self.reg.intervals(self.apri, False, self.recursively))

    def getNextBlk(self):

        try:
            return self.reg.ramBlkByN(self.apri, self.curr_n, self.recursively)

        except DataNotFoundError:
            return self.reg.diskBlkByN(self.apri, self.curr_n, self.recursively, **self.kwargs)

    def __iter__(self):
        return self

    def __next__(self):

        if self.stop is not None and self.curr_n >= self.stop:
            raise StopIteration

        elif self.curr_blk is None:

            self.intervals = self.reg.intervals(self.apri, False, self.recursively)
            self.curr_n = max( self.intervals[0][0] , self.curr_n )

            try:
                self.curr_blk = self.getNextBlk()

            except DataNotFoundError:
                raise StopIteration

        elif self.curr_n not in self.curr_blk:

            try:
                self.curr_blk = self.getNextBlk()

            except DataNotFoundError:

                self.intervals = self.reg.intervals(self.apri, False, self.recursively)

                for startn, length in self.intervals:

                    if startn > self.curr_n:

                        self.curr_n += math.ceil( (startn - self.curr_n) / self.step ) * self.step
                        break

                else:
                    raise StopIteration

                self.curr_blk = self.getNextBlk()

        ret = self.curr_blk[self.curr_n]
        self.curr_n += self.step
        return ret