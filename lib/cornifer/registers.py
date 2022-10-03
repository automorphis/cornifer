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
from contextlib import contextmanager, ExitStack
from pathlib import Path
from abc import ABC, abstractmethod

import lmdb
import numpy as np

from .errors import DataNotFoundError, RegisterAlreadyOpenError, RegisterError, CompressionError, \
    DecompressionError, NOT_ABSOLUTE_ERROR_MESSAGE, RegisterRecoveryError
from .info import ApriInfo, AposInfo
from .blocks import Block, MemmapBlock, ReleaseBlock
from .filemetadata import FileMetadata
from ._utilities import randomUniqueFilename, isInt, resolvePath, BYTES_PER_MB, isDeletable
from ._utilities.lmdb import lmdbHasKey, lmdbPrefixIter, openLmdb, lmdbIsClosed, lmdbCountKeys, \
    ReversibleTransaction
from .regfilestructure import VERSION_FILEPATH, LOCAL_DIR_CHARS, \
    COMPRESSED_FILE_SUFFIX, MSG_FILEPATH, CLS_FILEPATH, checkRegStructure, DATABASE_FILEPATH, \
    REG_FILENAME, MAP_SIZE_FILEPATH
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

def _blkNotFoundErrMsg(diskonly, apri, n = None, startn = None, length = None):

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
    "Exceeded max `Register` size of {0} Bytes. Please increase the max size using the method `increaseRegSize`."
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

class Register(ABC):

    #################################
    #     PUBLIC INITIALIZATION     #

    def __init__(self, savesDir, msg, initialRegSize = None):
        """
        :param savesDir: (type `str`)
        :param msg: (type `str`) A brief message describing this `Register`.
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
        self._dbMapSizeFilepath = None
        self._readonly = None

        self._version = CURRENT_VERSION
        self._versionFilepath = None

        self._clsFilepath = None

        self._startnHead = _START_N_HEAD_DEFAULT
        self._startnTailLength = _START_N_TAIL_LENGTH_DEFAULT
        self._startnTailMod = 10 ** self._startnTailLength
        self._lengthLength = _LENGTH_LENGTH_DEFAULT
        self._maxLength = _MAX_LENGTH_DEFAULT

        self._ramBlks = {}

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
                cls_name = fh.readline()

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

            with (localDir / MAP_SIZE_FILEPATH).open("r") as fh:
                mapSize = int(fh.readline())

            reg = con(localDir.parent, msg, mapSize)

            reg._setLocalDir(localDir)

            with (localDir / VERSION_FILEPATH).open("r") as fh:
                reg._version = fh.readline()

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
        return iter(self.apris())

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
        """Set the range of the `startn_` parameters of disk `Block`s belonging to this `Register`.

        Reset to default `head` and `tail_length` by omitting the parameters.

        If the `startn_` parameter is very large (of order more than trillions), then the `Register` database can
        become very bloated by storing many redundant digits for the `startn_` parameter. Calling this method with
        appropriate `head` and `tail_length` parameters alleviates the bloat.

        The "head" and "tail" of a non-negative number x is defined by x = head * 10^L + tail, where L is the "length_",
        or the number of digits, of "tail". (L must be at least 1, and 0 is considered to have 1 digit.)

        By calling `setstartnInfo(head, tail_length)`, the user is asserting that the startn_ of every disk
        `Block` belong to this `Register` can be decomposed in the fashion startn_ = head * 10^tail_length + tail. The
        user is discouraged to call this method for large `tail_length` values (>12), as this is likely unnecessary and
        defeats the purpose of this method.

        :param head: (type `int`, optional) Non-negative. If omitted, resets this `Register` to the default `head`.
        :param tailLen: (type `int`) Positive. If omitted, resets this `Register` to the default `tail_length`.
        """

        # DEBUG : 1, 2

        self._checkOpenRaise("setStartnInfo")

        self._checkReadwriteRaise("setStartnInfo")

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
                        "The following `startn_` does not have the correct head:\n" +
                        f"`startn_`   : {startn}\n" +
                        "That `startn_` is associated with a `Block` whose `ApriInfo` and length_ is:\n" +
                        f"`ApriInfo` : {str(apri.toJson())}\n" +
                        f"length_      : {length}\n"
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
                    fh.write(self._version + "\nDO NOT EDIT THIS FILE. CALL THE FUNCTION `updateRegVersion` INSTEAD.")

                with (localDir / CLS_FILEPATH).open("x") as fh:
                    fh.write(str(type(self).__name__) + "\nDO NOT EDIT THIS FILE.")

                with (localDir / MAP_SIZE_FILEPATH).open("x") as fh:
                    fh.write(str(self._dbMapSize) + "\nDO NOT EDIT THIS FILE. CALL THE FUNCTION `increaseRegSize` INSTEAD.")

                (localDir / DATABASE_FILEPATH).mkdir(exist_ok = False)
                self._setLocalDir(localDir)
                self._db = openLmdb(self._dbFilepath, self._dbMapSize, False)

                try:

                    with self._db.begin(write = True) as txn:
                        # set register info
                        txn.put(_START_N_HEAD_KEY, str(self._startnHead).encode("ASCII"))
                        txn.put(_START_N_TAIL_LENGTH_KEY, str(self._startnTailLength).encode("ASCII"))
                        txn.put(_LENGTH_LENGTH_KEY, str(_LENGTH_LENGTH_DEFAULT).encode("ASCII"))
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

    @staticmethod
    @contextmanager
    def opens(*regs, **kwargs):

        if not isinstance(regs, (list, tuple)):
            raise TypeError("`regs` must of type `list` or `tuple`.")

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

        with self._dbMapSizeFilepath.open("w") as fh:
            fh.write(str(self._dbMapSize))

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

        ret._readonly = readonly

        ret._db = openLmdb(ret._dbFilepath, ret._dbMapSize, readonly)

        with ret._db.begin() as txn:
            ret._lengthLength = int(txn.get(_LENGTH_LENGTH_KEY))

        ret._maxLength = 10 ** ret._lengthLength - 1

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
        self._dbMapSizeFilepath = localDir / MAP_SIZE_FILEPATH

    def _hasCompatibleVersion(self):
        return self._version in COMPATIBLE_VERSIONS

    def _dbIsClosed(self):

        if not self._created:
            raise RegisterError(_NOT_CREATED_ERROR_MESSAGE.format("_dbIsClosed"))

        else:
            return lmdbIsClosed(self._db)

    #################################
    #      PUBLIC APRI METHODS      #

    def apris(self, diskonly = False, recursively = False):

        self._checkOpenRaise("apris")

        if not isinstance(recursively, bool):
            raise TypeError("`recursively` must be of type `bool`")

        ret = []

        if not diskonly:
            ret.extend(self._ramBlks.keys())

        with lmdbPrefixIter(self._db, _ID_APRI_KEY_PREFIX) as it:

            for _, val in it:
                ret.append(ApriInfo.fromJson(val.decode("ASCII")))


        if recursively:

            for subreg in self._iterSubregs():

                with subreg._recursiveOpen(True) as subreg:
                    ret.append(subreg.apris())

        return sorted(list(set(ret)))

    def changeApri(self, oldApri, newApri, recursively = False):
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

        `some_other_apri = ApriInfo(descr = "period length_", respective = old_apri)`.

        After a call to `changeApri(old_apri, new_apri)`, the first `Block` will have `new_apri` and the second
        will have

        `ApriInfo(descr = "period length_", respective = new_apri)`.

        :param oldApri: (type `ApriInfo`)
        :param newApri: (type `ApriInfo`)
        :param recursively: (type `bool`)
        :raise ValueError: See above.
        """

        # DEBUG : 1, 2, 3

        self._checkOpenRaise("changeApri")

        self._checkReadwriteRaise("changeApri")

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
                    subreg.changeApri(oldApri, newApri, True)

    def rmvApri(self, apri, force = False, missingOk = False):
        """Remove an `ApriInfo` that is not associated with any other `ApriInfo`, `Block`, nor `AposInfo`.

        :param apri: (type `ApriInfo`)
        :raise ValueError: If there are any `ApriInfo`, `Block`, or `AposInfo` associated with `apri`.
        """

        # DEBUG : 1, 2, 3, 4, 5

        self._checkOpenRaise("rmvApri")

        self._checkReadwriteRaise("rmvApri")

        if not isinstance(apri, ApriInfo):
            raise TypeError("`apri` must be of type `ApriInfo`.")

        if not isinstance(force, bool):
            raise TypeError("`force` must be of type `bool`.")

        if not isinstance(missingOk, bool):
            raise TypeError("`missingOk` must be of type `bool`.")

        if not missingOk:
            self._checkKnownApri(apri)

        txn = None
        ramBlkDelSuccess = False
        reinsert = None

        try:

            if apri in self._ramBlks.keys():

                reinsert = self._ramBlks[apri]
                del self._ramBlks[apri]
                ramBlkDelSuccess = True

            with ReversibleTransaction(self._db).begin(write = True) as txn:
                blkDatas = self._rmvApriTxn(apri, force, txn)

            for apri_, startn, length in blkDatas:
                self.rmvDiskBlk(apri_, startn, length, False, False)

        except BaseException as e:

            if ramBlkDelSuccess:
                self._ramBlks[apri] = reinsert

            if txn is not None:

                with self._db.begin(write = True) as txn_:
                    txn.reverse(txn_)

            if isinstance(e, lmdb.MapFullError):
                raise RegisterError(_MEMORY_FULL_ERROR_MESSAGE.format(self._dbMapSize)) from e

            else:
                raise e

    #################################
    #      PROTEC APRI METHODS      #

    def _checkKnownApri(self, apri):

        try:
            self._getIdByApri(apri, None, False, None)

        except DataNotFoundError:
            pass

        else:
            return

        if apri not in self._ramBlks.keys():
            raise DataNotFoundError(_NO_APRI_ERROR_MESSAGE.format(str(apri)))

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
        :raises DataNotFoundError: If `apri` or `apriJson` is not known to this `Register` and `missingOk
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

                if 8 + 6 + len(key) > 4096:
                    warnings.warn(f"Long `ApriInfo` result in disk memory inefficiency. Long `ApriInfo`: {str(apri)}.")

                return _id

            else:

                if apri is None:
                    apri = ApriInfo.fromJson(apriJson.decode("ASCII"))

                raise DataNotFoundError(_NO_APRI_ERROR_MESSAGE.format(str(apri)))

        finally:

            if commit:

                try:
                    txn.commit()

                except lmdb.MapFullError as e:
                    raise RegisterError(_MEMORY_FULL_ERROR_MESSAGE.format(self._dbMapSize)) from e

    def _rmvApriTxn(self, apri, force, txn):

        apris = []
        aposs = []
        blkDatas = []
        self._rmvApriTxnHelper(txn, apri, apris, aposs, blkDatas, force)

        if force:

            for data in blkDatas:
                self._rmvDiskBlkTxn(data[0], data[1], data[2], txn)

            for apri in aposs:
                self._rmvAposInfoTxn(apri, txn)

            for apri in apris:
                self._rmvApriTxn(apri, False, txn)

        apriJson = apri.toJson().encode()
        apriId = self._getIdByApri(apri, apriJson, False, txn)
        txn.delete(_ID_APRI_KEY_PREFIX + apriId)
        txn.delete(_APRI_ID_KEY_PREFIX + apriJson)

        return blkDatas

    def _rmvApriTxnHelper(self, txn, apri, apris, aposs, blkDatas, force):

        apris.append(apri)

        if _debug == 1:
            raise KeyboardInterrupt

        if self._numDiskBlksTxn(apri, txn) != 0:

            if not force:

                raise ValueError(
                    f"There are disk `Block`s saved with `{str(apri)}`. Please remove them first and call "
                    "`rmvApri` again. Or remove them automatically by calling "
                    "`reg.rmvApri(apri, force = True)`."
                )

            else:

                for key, val in self._iterDiskBlkPairs(_BLK_KEY_PREFIX, apri, None, txn):

                    blkFilename = val.decode("ASCII")

                    if not isDeletable(self._localDir / blkFilename):
                        raise OSError(f"Cannot delete `Block` file `{blkFilename}`.")

                    blkDatas.append(self._convertDiskBlockKey(_BLK_KEY_PREFIX_LEN, key))

                for key, val in self._iterDiskBlkPairs(_COMPRESSED_KEY_PREFIX, apri, None, txn):

                    comprFilename = val.decode("ASCII")

                    if val != _IS_NOT_COMPRESSED_VAL and not isDeletable(self._localDir / comprFilename):
                        raise OSError(f"Cannot delete compressed `Block` file `{comprFilename}`.")

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
                    "`rmvApri` again. Or remove automatically by calling `reg.rmvApri(apri, force = True)`."
                )

            else:
                aposs.append(apri)

        if _debug == 3:
            raise KeyboardInterrupt

        with lmdbPrefixIter(txn, _ID_APRI_KEY_PREFIX) as it:

            for _, _apri_json in it:

                _apri = ApriInfo.fromJson(_apri_json.decode("ASCII"))

                if apri in _apri and apri != _apri:

                    if not force:

                        raise ValueError(
                            f"{str(_apri)} is associated with {str(apri)}. Please remove the former first before "
                            f"removing the latter. Or remove automatically by calling `reg.rmvApri(apri, "
                            f"force = True)`."
                        )

                    else:
                        self._rmvApriTxnHelper(txn, _apri, apris, aposs, blkDatas, True)

            if _debug == 4:
                raise KeyboardInterrupt

    #################################
    #      PUBLIC APOS METHODS      #

    def setApos(self, apri, apos):
        """Set some `AposInfo` for corresponding `ApriInfo`.

        WARNING: This method will OVERWRITE any previous saved `AposInfo`. If you do not want to lose any previously
        saved data, then you should do something like the following:

            apos = reg.apos(apri)
            apos.period_length = 5
            reg.setApos(apos)

        :param apri: (type `ApriInfo`)
        :param apos: (type `AposInfo`)
        """

        # DEBUG : 1, 2

        self._checkOpenRaise("setApos")

        self._checkReadwriteRaise("setApos")

        if not isinstance(apri, ApriInfo):
            raise TypeError("`apri` must be of type `ApriInfo`")

        if not isinstance(apos, AposInfo):
            raise TypeError("`apos` must be of type `AposInfo`")

        if _debug == 1:
            raise KeyboardInterrupt

        try:

            with self._db.begin(write = True) as txn:

                self._setAposInfoTxn(apri, apos, txn)

                if _debug == 2:
                    raise KeyboardInterrupt

        except lmdb.MapFullError as e:
            raise RegisterError(_MEMORY_FULL_ERROR_MESSAGE.format(self._dbMapSize)) from e

    def apos(self, apri):
        """Get some `AposInfo` associated with a given `ApriInfo`.

        :param apri: (type `ApriInfo`)
        :raises DataNotFoundError: If no `AposInfo` has been associated to `apri`.
        :return: (type `AposInfo`)
        """

        self._checkOpenRaise("apos")

        if not isinstance(apri, ApriInfo):
            raise TypeError("`apri` must be of type `ApriInfo`")

        key = self._getAposKey(apri, None, False)

        with self._db.begin() as txn:
            apos_json = txn.get(key, default=None)

        if apos_json is not None:
            return AposInfo.fromJson(apos_json.decode("ASCII"))

        else:
            raise DataNotFoundError(f"No `AposInfo` associated with `{str(apri)}`.")

    def rmvApos(self, apri, missingOk = False):

        # DEBUG : 1, 2

        self._checkOpenRaise("rmvApos")

        self._checkReadwriteRaise("rmvApos")

        if not isinstance(apri, ApriInfo):
            raise TypeError("`apri` must be of type `ApriInfo`.")

        if not isinstance(missingOk, bool):
            raise TypeError("`missingOk` must be of type `bool`.")

        if _debug == 1:
            raise KeyboardInterrupt

        try:

            with self._db.begin(write = True) as txn:

                self._rmvAposInfoTxn(apri, txn)

                if _debug == 2:
                    raise KeyboardInterrupt

        except lmdb.MapFullError as e:
            raise RegisterError(_MEMORY_FULL_ERROR_MESSAGE.format(self._dbMapSize)) from e

        except DataNotFoundError:

            if not missingOk:
                raise

    #################################
    #      PROTEC APOS METHODS      #

    def _setAposInfoTxn(self, apri, apos, txn):

        key = self._getAposKey(apri, None, True, txn)
        apos_json = apos.toJson().encode("ASCII")
        txn.put(key, apos_json)

        if 6 + 8 + _APOS_KEY_PREFIX_LEN +  len(apos_json) > 4096:
            warnings.warn(f"Long `AposInfo` result in disk memory inefficiency. Long `AposInfo`: {str(apri)}.")

    def _rmvAposInfoTxn(self, apri, txn):

        key = self._getAposKey(apri, None, False, txn)

        if lmdbHasKey(txn, key):
            txn.delete(key)

        else:
            raise DataNotFoundError(f"No `AposInfo` associated with `{str(apri)}`.")

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

    def addSubreg(self, subreg, existsOk = False):

        # DEBUG : 1, 2

        self._checkOpenRaise("addSubreg")

        self._checkReadwriteRaise("addSubreg")

        if not isinstance(subreg, Register):
            raise TypeError("`subreg` must be of a `Register` derived type")

        if not subreg._created:
            raise RegisterError(_NOT_CREATED_ERROR_MESSAGE.format("addSubreg"))

        if _debug == 1:
            raise KeyboardInterrupt

        try:

            with self._db.begin(write = True) as txn:
                self._addSubregTxn(subreg, txn)

        except lmdb.MapFullError as e:
            raise RegisterError(_MEMORY_FULL_ERROR_MESSAGE.format(self._dbMapSize)) from e

        except RegisterError as e:

            if str(e) == _REG_ALREADY_ADDED_ERROR_MESSAGE:

                if not existsOk:
                    raise

            else:
                raise

    def rmvSubreg(self, subreg, missingOk = False):
        """
        :param subreg: (type `Register`)
        """

        # DEBUG : 1, 2

        self._checkOpenRaise("rmvSubreg")

        self._checkReadwriteRaise("rmvSubreg")

        if not isinstance(subreg, Register):
            raise TypeError("`subreg` must be of a `Register` derived type.")

        if not isinstance(missingOk, bool):
            raise TypeError("`missingOk` must be of type `bool`.")

        if _debug == 1:
            raise KeyboardInterrupt

        try:

            with self._db.begin(write = True) as txn:

                self._rmvSubregTxn(subreg, txn)

                if _debug == 2:
                    raise KeyboardInterrupt

        except lmdb.MapFullError as e:
            raise RegisterError(_MEMORY_FULL_ERROR_MESSAGE.format(self._dbMapSize)) from e

        except DataNotFoundError:

            if not missingOk:
                raise

    def subregs(self):
        return list(self._iterSubregs())

    #################################
    #  PROTEC SUB-REGISTER METHODS  #

    def _addSubregTxn(self, subreg, txn):

        key = subreg._getSubregKey()

        if not lmdbHasKey(txn, key):

            if subreg._checkNoCyclesFrom(self):

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

    def _rmvSubregTxn(self, subreg, txn):

        key = subreg._getSubregKey()

        if lmdbHasKey(txn, key):
            txn.delete(key)

        else:
            raise RegisterError(f"No subregister found with the following message : {str(subreg)}")

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

        Most use-cases prefer the method `blk`.

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

        :param filename: (type `pathlib.Path`) Where to remove the raw data.
        :raises DataNotFoundError: If the data could not be cleaned because it doesn't exist.
        """

        if not filename.is_absolute():
            raise ValueError(NOT_ABSOLUTE_ERROR_MESSAGE.format(filename))

        filename.unlink(missing_ok = False)

    @classmethod
    @abstractmethod
    def withSuffix(cls, filename):
        """Adds a suffix to a filename and returns it.

        :param filename: (type `pathlib.Path`)
        :return: (type `pathlib.Path`)
        """

    def addDiskBlk(self, blk, existsOk = False, retMetadata = False, **kwargs):
        """Save a `Block` to disk and link it with this `Register`.

        :param blk: (type `Block`)
        :param retMetadata: (type `bool`, default `False`) Whether to return a `File_Metadata` object, which
        contains file creation date/time and size of dumped data to the disk.
        :raises RegisterError: If a duplicate `Block` already exists in this `Register`.
        """

        #DEBUG : 1, 2, 3, 4, 5

        self._checkOpenRaise("addDiskBlk")
        self._checkReadwriteRaise("addDiskBlk")

        if not isinstance(blk, Block):
            raise TypeError("`blk` must be of type `Block`.")

        if not isinstance(existsOk, bool):
            raise TypeError("`existsOk` must be of type `bool`.")

        if not isinstance(retMetadata, bool):
            raise TypeError("`retMetadata` must be of type `bool`.")

        startnHead = blk.startn() // self._startnTailMod

        if startnHead != self._startnHead :

            raise IndexError(
                "The `startn_` for the passed `Block` does not have the correct head:\n"
                f"`tailLen`       : {self._startnTailLength}\n"
                f"expected `head` : {self._startnHead}\n"
                f"`startn_`       : {blk.startn()}\n"
                f"`startn_` head  : {startnHead}\n"
                "Please see the method `setStartnInfo` to troubleshoot this error."
            )

        if len(blk) > self._maxLength:
            raise ValueError

        if _debug == 1:
            raise KeyboardInterrupt

        txn = None
        filename = None

        if _debug == 2:
            raise KeyboardInterrupt

        try:

            with ReversibleTransaction(self._db).begin(write = True) as txn:

                filename = self._addDiskBlkTxn(blk, txn)

                if _debug == 3:
                    raise KeyboardInterrupt

            if _debug == 4:
                raise KeyboardInterrupt

            type(self).dumpDiskData(blk.segment(), filename, **kwargs)

            if _debug == 5:
                raise KeyboardInterrupt

        except BaseException as e:

            if not isinstance(e, RegisterError) or not existsOk or not "exist" in str(e):

                try:

                    if filename is not None:
                        filename.unlink(missing_ok = True)

                    if txn is not None:

                        with self._db.begin(write = True) as txn_:
                            txn.reverse(txn_)

                except:
                    raise RegisterRecoveryError("Could not successfully recover from a failed disk `Block` add!") from e

                else:

                    if isinstance(e, lmdb.MapFullError):
                        raise RegisterError(_MEMORY_FULL_ERROR_MESSAGE.format(self._dbMapSize)) from e

                    else:
                        raise e

        if retMetadata:
            return self.blkMetadata(blk.apri(), blk.startn(), len(blk), False)

        else:
            return None

    def appendDiskBlk(self, blk, existsOk = False, retMetadata = False, **kwargs):
        """Add a `Block` to disk and link it with this `Register`.

        If no disk `Block`s with the same `ApriInfo` as `blk` have previously been added to disk, then `blk` will be
        added to this `Register` via the method `addDiskBlk`. If not, then `startn_` will be set to one more than the
        largest index among all disk `Block`s with the same `ApriInfo` as `blk`.

        :param blk: (type `Block`)
        :param retMetadata: (type `bool`, default `False`) Whether to return a `File_Metadata` object, which
        contains file creation date/time and size of dumped data to the disk.
        :raises RegisterError: If a duplicate `Block` already exists in this `Register`.
        """

        if not isinstance(blk, Block):
            raise TypeError("`blk` must be of type `Block`.")

        if not isinstance(existsOk, bool):
            raise TypeError("`existsOk` must be of type `bool`.")

        if not isinstance(retMetadata, bool):
            raise TypeError("`retMetadata` must be of type `bool`.")

        if self.numBlks(blk.apri(), diskonly = True) == 0:
            return self.addDiskBlk(blk, existsOk, retMetadata, **kwargs)

        else:

            startn = 0

            for key, _ in self._iterDiskBlkPairs(_BLK_KEY_PREFIX, blk.apri(), None):

                _, _startn, _length = self._convertDiskBlockKey(_BLK_KEY_PREFIX_LEN, key)

                if startn < _startn + _length:
                    startn = _startn + _length

            blk.setStartn(startn)
            return self.addDiskBlk(blk, existsOk, retMetadata, **kwargs)

    def rmvDiskBlk(self, apri, startn = None, length = None, missingOk = False, recursively = False, **kwargs):
        """Delete a disk `Block` and unlink it with this `Register`.

        :param apri: (type `ApriInfo`)
        :param startn_: (type `int`) Non-negative.
        :param length_: (type `int`) Non-negative.
        :param recursively: (type `bool`)
        """

        # DEBUG : 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17

        self._checkOpenRaise("rmvDiskBlk")
        self._checkReadwriteRaise("rmvDiskBlk")
        startn, length = Register._checkApriStartnLengthRaise(apri, startn, length)

        try:
            startn_, length_ = self._resolveStartnLength(apri, startn, length, True)

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

                    blkFilename, comprFilename = self._rmvDiskBlkTxn(apri, startn_, length_, txn)

                    if _debug == 5:
                        raise KeyboardInterrupt

                if _debug == 6:
                    raise KeyboardInterrupt

                if not isDeletable(blkFilename):
                    raise OSError(f"Cannot delete `Block` file `{str(blkFilename)}`.")

                if _debug == 7:
                    raise KeyboardInterrupt

                if comprFilename is not None and not isDeletable(comprFilename):
                    raise OSError(f"Cannot delete compressed `Block` file `{str(comprFilename)}`.")

                if _debug == 8:
                    raise KeyboardInterrupt

                if comprFilename is not None:

                    blkFilename.unlink(missing_ok = False)

                    if _debug == 9:
                        raise KeyboardInterrupt

                    comprFilename.unlink(missing_ok = False)

                else:
                    type(self).cleanDiskData(blkFilename, **kwargs)

                return

            except BaseException as e:

                FAIL_NO_RECOVER_ERROR_MESSAGE = "Could not successfully recover from a failed disk `Block` remove!"

                try:

                    if comprFilename is not None:

                        if blkFilename is not None and comprFilename.exists():
                            blkFilename.touch(exist_ok=True)

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
                        raise RegisterError(_MEMORY_FULL_ERROR_MESSAGE.format(self._dbMapSize)) from e

                    else:
                        raise e

        if recursively:

            for subreg in self._iterSubregs():

                with subreg._recursiveOpen(False) as subreg:

                    try:
                        subreg.rmvDiskBlk(apri, startn, length, False, True, **kwargs)
                        return

                    except DataNotFoundError:
                        pass

        if not missingOk:
            raise DataNotFoundError(_blkNotFoundErrMsg(True, apri, None, startn, length))

    def blkMetadata(self, apri, startn = None, length = None, recursively = False):

        self._checkOpenRaise("blkMetadata")

        startn, length = Register._checkApriStartnLengthRaise(apri, startn, length)

        if not isinstance(recursively, bool):
            raise TypeError("`recursively` must be of type `bool`.")

        startn_, length_ = self._resolveStartnLength(apri, startn, length, True)

        try:
            blk_key, compressed_key = self._checkBlkCompressedKeysRaise(None, None, apri, None, startn_, length_)

        except DataNotFoundError:
            pass

        else:
            blk_filename, compressed_filename = self._checkBlkCompressedFilesRaise(
                blk_key, compressed_key, apri, startn_, length_
            )

            if compressed_filename is not None:
                return FileMetadata.fromPath(compressed_filename)

            else:
                return FileMetadata.fromPath(blk_filename)

        if recursively:

            for subreg in self._iterSubregs():

                with subreg._recursiveOpen(True) as subreg:

                    try:
                        return subreg.blkMetadata(apri, startn_, length_, True)

                    except DataNotFoundError:
                        pass

        raise DataNotFoundError(
            _blkNotFoundErrMsg(True, apri, None, startn, length)
        )

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

        startn, length = Register._checkApriStartnLengthRaise(apri, startn, length)

        if not isInt(compressionLevel):
            raise TypeError("`compressionLevel` must be of type `int`.")
        else:
            compressionLevel = int(compressionLevel)

        if not isinstance(retMetadata, bool):
            raise TypeError("`retMetadata` must be of type `bool`.")

        if not (0 <= compressionLevel <= 9):
            raise ValueError("`compressionLevel` must be between 0 and 9, inclusive.")

        startn_, length_ = self._resolveStartnLength(apri, startn, length, True)

        compressed_key = self._getDiskBlkKey(
            _COMPRESSED_KEY_PREFIX, apri, None, startn_, length_, False
        )

        blk_key, compressed_key = self._checkBlkCompressedKeysRaise(
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

        startn, length = Register._checkApriStartnLengthRaise(apri, startn, length)

        if not isinstance(retMetadata, bool):
            raise TypeError("`retMetadata` must be of type `bool`.")

        startn_, length_ = self._resolveStartnLength(apri, startn, length, True)

        blk_key, compressed_key = self._checkBlkCompressedKeysRaise(None, None, apri, None, startn_, length_)

        with self._db.begin() as txn:
            compressed_val = txn.get(compressed_key)

        if compressed_val == _IS_NOT_COMPRESSED_VAL:

            raise DecompressionError(
                "The disk `Block` with the following data is not compressed: " +
                f"{str(apri)}, startn = {startn_}, length = {length_}"
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

    def isCompressed(self, apri, startn = None, length = None):

        self._checkOpenRaise("isCompressed")

        self._checkApriStartnLengthRaise(apri, startn, length)

        startn_, length_ = self._resolveStartnLength(apri, startn, length, True)

        with self._db.begin() as txn:
            return txn.get(
                self._getDiskBlkKey(_COMPRESSED_KEY_PREFIX, apri, None, startn_, length_, False, txn)
            ) != _IS_NOT_COMPRESSED_VAL

    # def setHashing(self, apri, hashing):
    #     """Enable or disable automatic hashing for disk `Block`s with `apri`.
    #
    #     If `hashing` is set to `True`, then every disk `Block` with `apri` added to this `Register` will be hashed. A
    #     `Block` is hashed by calling `hash` on each of its entries. The hashes are saved to a hash-set on disk.
    #
    #     If `hashing` is set to `False`, then all hashes associated to `apri` will be DELETED (if they exist) and no
    #     future hashes are calculated.
    #
    #     For best results, set hashing to `True` only before adding any disk `Block`s with `apri`.
    #
    #     :param hashing: (type `bool`)
    #     """

    #################################
    #    PROTEC DISK BLK METHODS    #

    def _addDiskBlkTxn(self, blk, txn):

        # DEBUG : 6,7,8,9, 10

        apris = [apri for _, apri in blk.apri().iterInnerInfo() if isinstance(apri, ApriInfo)]

        if _debug == 6:
            raise KeyboardInterrupt

        # this will create ID's if necessary
        for i, apri in enumerate(apris):
            self._getIdByApri(apri, None, True, txn)

        if _debug == 7:
            raise KeyboardInterrupt

        blkKey = self._getDiskBlkKey(
            _BLK_KEY_PREFIX,
            blk.apri(), None, blk.startn(), len(blk),
            False, txn
        )

        if not lmdbHasKey(txn, blkKey):

            filename = randomUniqueFilename(self._localDir, length=6)

            if _debug == 8:
                raise KeyboardInterrupt

            filename = type(self).withSuffix(filename)

            if _debug == 9:
                raise KeyboardInterrupt

            filename_bytes = str(filename.name).encode("ASCII")
            compressed_key = _COMPRESSED_KEY_PREFIX + blkKey[_BLK_KEY_PREFIX_LEN:]

            txn.put(blkKey, filename_bytes)
            txn.put(compressed_key, _IS_NOT_COMPRESSED_VAL)

            if _debug == 10:
                raise KeyboardInterrupt

            if len(blk) == 0:

                warnings.warn(
                    "Added a length_ 0 disk `Block` to this `Register`.\n" +
                    f"`Register` msg: {str(self)}\n" +
                    f"`Block`: {str(blk)}\n" +
                    f"`Register` location: {str(self._localDir)}"
                )

            return filename

        else:

            raise RegisterError(
                f"Duplicate `Block` with the following data already exists in this `Register`: " +
                f"{str(blk.apri())}, startn_ = {blk.startn()}, length_ = {len(blk)}."
            )

    def _rmvDiskBlkTxn(self, apri, startn, length, txn):

        if _debug == 12:
            raise KeyboardInterrupt

        # raises DataNotFoundError
        self._getIdByApri(apri, None, False, txn)

        if _debug == 13:
            raise KeyboardInterrupt

        # raises RegisterError and DataNotFoundError
        blk_key, compressed_key = self._checkBlkCompressedKeysRaise(None, None, apri, None, startn, length, txn)

        if _debug == 14:
            raise KeyboardInterrupt

        blkFilename, comprFilename = self._checkBlkCompressedFilesRaise(
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

        return blkFilename, comprFilename

    def _getDiskBlkKey(self, prefix, apri, apriJson, startn, length, missingOk, txn = None):
        """Get the database key for a disk `Block`.

        One of `apri` and `apriJson` can be `None`, but not both. If both are not `None`, then `apri` is used.
        `self._db` must be opened by the caller. This method only queries the database to obtain the `apri` ID.

        If `missingOk is True` and an ID for `apri` does not already exist, then a new one will be created. If
        `missingOk is False` and an ID does not already exist, then an error is raised.

        :param prefix: (type `bytes`)
        :param apri: (type `ApriInfo`)
        :param apriJson: (types `bytes`)
        :param startn: (type `int`) The start index of the `Block`.
        :param length: (type `int`) The length_ of the `Block`.
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
        tail = f"{tail:0{self._startnTailLength}d}".encode("ASCII")
        opLength = self._maxLength - length
        opLength = f"{opLength:0{self._lengthLength}d}".encode("ASCII")

        return (
                prefix   +
                _id      + _KEY_SEP +
                tail     + _KEY_SEP +
                opLength
        )

    def _numDiskBlksTxn(self, apri, txn):

        try:

            return lmdbCountKeys(
                txn,
                _BLK_KEY_PREFIX + self._getIdByApri(apri, None, False) + _KEY_SEP
            )

        except DataNotFoundError:
            return 0

    def _iterDiskBlkPairs(self, prefix, apri, apriJson, txn = None):
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

        try:
            prefix += self._getIdByApri(apri, apriJson, False, txn)

        except DataNotFoundError:
            pass

        else:

            prefix += _KEY_SEP

            with lmdbPrefixIter(txn if txn is not None else self._db, prefix) as it:
                yield from it

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
        :return (type `int`) startn_
        :return (type `int`) length_, non-negative
        """

        apri_id, startn_bytes, opLength_bytes = Register._splitDiskBlockKey(prefixLen, key)

        if apri is None:

            apri_json = self._getApriJsonById(apri_id, txn)
            apri = ApriInfo.fromJson(apri_json.decode("ASCII"))

        return (
            apri,
            int(startn_bytes.decode("ASCII")) + self._startnHead * self._startnTailMod,
            self._maxLength - int(opLength_bytes.decode("ASCII"))
        )

    def _checkBlkCompressedKeysRaise(self, blkKey, compressedKey, apri, apriJson, startn, length, txn = None):

        if compressedKey is None and blkKey is None:
            compressedKey = self._getDiskBlkKey(_COMPRESSED_KEY_PREFIX, apri, apriJson, startn, length, False, txn)

        if blkKey is not None and compressedKey is None:
            compressedKey = _COMPRESSED_KEY_PREFIX + blkKey[_BLK_KEY_PREFIX_LEN:]

        elif compressedKey is not None and blkKey is None:
            blkKey = _BLK_KEY_PREFIX + compressedKey[_COMPRESSED_KEY_PREFIX_LEN:]

        if apri is None:
            apri = ApriInfo.fromJson(apriJson.decode("ASCII"))

        if txn is None:
            txn = self._db

        hasBlkKey = lmdbHasKey(txn, blkKey)
        hasComprKey = lmdbHasKey(txn, compressedKey)

        if (not hasBlkKey and hasComprKey) or (hasBlkKey and not hasComprKey):
            raise RegisterError("Uncompressed/compressed `Block` key mismatch.")

        if not hasBlkKey:
            raise DataNotFoundError(
                _blkNotFoundErrMsg(True, apri, None, startn, length)
            )

        return blkKey, compressedKey

    def _checkBlkCompressedFilesRaise(self, blkKey, compressedKey, apri, startn, length, txn = None):

        commit = txn is None

        try:

            if commit:
                txn = self._db.begin()

            blk_val = txn.get(blkKey)
            compressed_val = txn.get(compressedKey)
            blk_filename = self._localDir / blk_val.decode("ASCII")

            if compressed_val != _IS_NOT_COMPRESSED_VAL:

                compressed_filename = self._localDir / compressed_val.decode("ASCII")

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

    @staticmethod
    def _checkApriStartnLengthRaise(apri, startn, length):

        if not isinstance(apri, ApriInfo):
            raise TypeError("`apri` must be of type `ApriInfo`")

        if not isInt(startn) and startn is not None:
            raise TypeError("startn_` must be an `int`")

        elif startn is not None:
            startn = int(startn)

        if not isInt(length) and length is not None:
            raise TypeError("`length_` must be an `int`")

        elif length is not None:
            length = int(length)

        if startn is not None and startn < 0:
            raise ValueError("`startn_` must be non-negative")

        if length is not None and length < 0:
            raise ValueError("`length_` must be non-negative")

        return startn, length

    #################################
    #    PUBLIC RAM BLK METHODS     #

    def addRamBlk(self, blk):

        self._checkOpenRaise("addRamBlk")

        if not isinstance(blk, Block):
            raise TypeError("`blk` must be of type `Block`.")

        if blk.apri() not in self._ramBlks.keys():
            self._ramBlks[blk.apri()] = [blk]

        elif len(self._ramBlks[blk.apri()]) == 0:
            self._ramBlks[blk.apri()].append(blk)

        else:

            for i, blk_ in enumerate(self._ramBlks[blk.apri()]):

                if blk_ is blk:
                    break

                elif blk.startn() < blk_.startn() or (blk.startn() == blk_.startn() and len(blk) > len(blk_)):

                    self._ramBlks[blk.apri()].insert(i, blk)
                    break

            else:
                self._ramBlks[blk.apri()].append(blk)

    def rmvRamBlk(self, blk):

        self._checkOpenRaise("addRamBlk")

        if not isinstance(blk, Block):
            raise TypeError("`blk` must be of type `Block`.")

        if blk.apri() not in self._ramBlks.keys():
            raise DataNotFoundError(f"No RAM `Block` found with the following data: {str(blk.apri())}.")

        for i, blk_ in enumerate(self._ramBlks[blk.apri()]):

            if blk_ is blk:

                del self._ramBlks[blk.apri()][i]

                if len(self._ramBlks[blk.apri()]) == 0:
                    del self._ramBlks[blk.apri()]

                return

        else:
            raise DataNotFoundError(f"No matching RAM disk `Block` found.")

    def rmvAllRamBlks(self):

        self._checkOpenRaise("rmvAllRamBlks")
        self._ramBlks = {}

    #################################
    #    PROTEC RAM BLK METHODS     #

    #################################
    # PUBLIC RAM & DISK BLK METHODS #

    def blkByN(self, apri, n, diskonly = False, recursively = False, retMetadata = False, **kwargs):

        self._checkOpenRaise("blkByN")

        if not isinstance(apri, ApriInfo):
            raise TypeError("`apri` must be of type `ApriInfo`.")

        if not isInt(n):
            raise TypeError("`n` must be of type `int`.")

        else:
            n = int(n)

        if not isinstance(diskonly, bool):
            raise TypeError("`diskonly` must be of type `bool`.")

        if not isinstance(recursively, bool):
            raise TypeError("`recursively` must be of type `bool`.")

        if not isinstance(retMetadata, bool):
            raise TypeError("`retMetadata` must be of type `bool`.")

        if n < 0:
            raise IndexError("`n` must be non-negative.")

        try:
            self._checkKnownApri(apri)

        except DataNotFoundError:

            if not recursively:
                raise

        else:

            for startn, length in self.intervals(apri, combine = False, diskonly = diskonly, recursively = False):

                if startn <= n < startn + length:
                    return self.blk(apri, startn, length, diskonly, False, retMetadata, **kwargs)

        if recursively:

            for subreg in self._iterSubregs():

                with subreg._recursiveOpen(True) as subreg:

                    try:
                        return subreg.blkByN(apri, n, diskonly, True, retMetadata, **kwargs)

                    except DataNotFoundError:
                        pass

        raise DataNotFoundError(_blkNotFoundErrMsg(diskonly, apri, n))

    def blk(self, apri, startn = None, length = None, diskonly = False, recursively = False, retMetadata = False, **kwargs):

        self._checkOpenRaise("blk")

        startn, length = Register._checkApriStartnLengthRaise(apri, startn, length)

        if not isinstance(diskonly, bool):
            raise TypeError("`diskonly` must be of type `bool`.")

        if not isinstance(recursively, bool):
            raise TypeError("`recursively` must be of type `bool`.")

        if not isinstance(retMetadata, bool):
            raise TypeError("`retMetadata` must be of type `bool`.")

        try:
            self._checkKnownApri(apri)

        except DataNotFoundError:

            if not recursively:
                raise

        else:

            startn_, length_ = self._resolveStartnLength(apri, startn, length, diskonly)

            if not diskonly and apri in self._ramBlks.keys():

                for blk in self._ramBlks[apri]:

                    if blk.startn() == startn_ and len(blk) == length_:

                        if retMetadata:
                            return blk, None

                        else:
                            return blk

            try:
                blk_key, compressed_key = self._checkBlkCompressedKeysRaise(None, None, apri, None, startn_, length_)

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

                blk_filename, _ = self._checkBlkCompressedFilesRaise(blk_key, compressed_key, apri, startn_, length_)
                blk_filename = self._localDir / blk_filename
                data = type(self).loadDiskData(blk_filename, **kwargs)
                blk = Block(data, apri, startn_)

                if retMetadata:
                    return blk, FileMetadata.fromPath(blk_filename)

                else:
                    return blk

        if recursively:

            for subreg in self._iterSubregs():

                with subreg._recursiveOpen(True) as subreg:

                    try:
                        return subreg.blk(apri, startn, length, retMetadata, retMetadata=True)

                    except DataNotFoundError:
                        pass

        raise DataNotFoundError(
            _blkNotFoundErrMsg(diskonly, str(apri), None, startn, length)
        )

    def blks(self, apri, sort = False, diskonly = False, recursively = False, retMetadata = False, **kwargs):

        self._checkOpenRaise("blks")

        if not isinstance(apri, ApriInfo):
            raise TypeError("`apri` must be of type `ApriInfo`.")

        if not isinstance(diskonly, bool):
            raise TypeError("`diskonly` must be of type `bool`.")

        if not isinstance(retMetadata, bool):
            raise TypeError("`retMetadata` must be of type `bool`.")

        if not isinstance(recursively, bool):
            raise TypeError("`recursively` must be of type `bool`.")

        try:
            self._checkKnownApri(apri)

        except DataNotFoundError:

            if not recursively:
                raise

        else:

            for startn, length in self.intervals(apri, combine = False, diskonly = diskonly, recursively = recursively):

                try:
                    yield self.blk(apri, startn, length, diskonly, False, retMetadata, **kwargs)

                except DataNotFoundError:
                    pass

        if recursively:

            for subreg in self._iterSubregs():

                with subreg._recursiveOpen(True) as subreg:
                    yield from subreg.blks(apri, sort, diskonly, True, retMetadata, **kwargs)

    def __getitem__(self, apriNDiskonlyRecursively):

        if not isinstance(apriNDiskonlyRecursively, tuple) or len(apriNDiskonlyRecursively) <= 1:
            raise TypeError("Must pass at least two arguments to `reg[]`.")

        if len(apriNDiskonlyRecursively) >= 5:
            raise TypeError("Must pass at most four arguments to `reg[]`.")

        if len(apriNDiskonlyRecursively) == 2:

            apri, n = apriNDiskonlyRecursively
            diskonly = False
            recursively = False

        elif len(apriNDiskonlyRecursively) == 3:

            apri, n, diskonly = apriNDiskonlyRecursively
            recursively = False

        else:
            apri, n, diskonly, recursively = apriNDiskonlyRecursively

        return self.get(apri, n, diskonly, recursively)

    def get(self, apri, n, diskonly = False, recursively = False, **kwargs):

        self._checkOpenRaise("get")

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
            self._checkKnownApri(apri)

        except DataNotFoundError:

            if not recursively:
                raise

        else:

            if isinstance(n, slice):
                # return iterator if given slice
                return _ElementIter(self, apri, n, diskonly, recursively, kwargs)

            else:

                blk = self.blkByN(apri, n, diskonly, recursively, False, **kwargs)
                ret = blk[n]

                if isinstance(blk, ReleaseBlock):
                    blk.release()

                return ret

        if recursively:

            for subreg in self._iterSubregs():

                with subreg._recursiveOpen(True) as subreg:

                    try:
                        return subreg.get(apri, n, diskonly, recursively, **kwargs)

                    except DataNotFoundError:
                        pass

        raise DataNotFoundError(
            _blkNotFoundErrMsg(diskonly, str(apri), n)
        )

    def intervals(self, apri, sort = False, combine = False, diskonly = False, recursively = False):

        self._checkOpenRaise("intervals")

        if not isinstance(apri, ApriInfo):
            raise TypeError("`apri` must be of type `ApriInfo`.")

        if not isinstance(sort, bool):
            raise TypeError("`sort` must be of type `bool`.")

        if not isinstance(combine, bool):
            raise TypeError("`combine` must be of type `bool`.")

        if not isinstance(diskonly, bool):
            raise TypeError("`diskonly` must be of type `bool`.")

        if not isinstance(recursively, bool):
            raise TypeError("`recursively` must be of type `bool`.")

        if not sort and not combine:

            try:
                self._checkKnownApri(apri)

            except DataNotFoundError:

                if not recursively:
                    raise

            else:

                if not diskonly and apri in self._ramBlks.keys():

                    for blk in self._ramBlks[apri]:
                        yield blk.startn(), len(blk)

                for key, _ in self._iterDiskBlkPairs(_BLK_KEY_PREFIX, apri, None):
                    yield self._convertDiskBlockKey(_BLK_KEY_PREFIX_LEN, key, apri)[1:]

            if recursively:

                for subreg in self._iterSubregs():

                    with subreg._recursiveOpen(True) as subreg:
                        yield from subreg.intervals(apri, sort = False, combine = False, diskonly = diskonly, recursively = True)

        elif combine:

            ret = []
            intervals_sorted = self.intervals(apri, sort = True, combine = False, diskonly = diskonly, recursively = recursively)
            for startn, length in intervals_sorted:

                if len(ret) == 0:
                    ret.append((startn, length))

                elif startn <= ret[-1][0] + ret[-1][1]:
                    ret[-1] = (ret[-1][0], max(startn + length - ret[-1][0], ret[-1][1]))

            yield from ret

        else:
            yield from sorted(
                list(self.intervals(apri, sort = False, combine = False, diskonly = diskonly, recursively = recursively)),
                key = lambda t: (t[0], -t[1])
            )

    def totalLen(self, apri, diskonly = False, recursively = False):

        self._checkOpenRaise("totalLen")

        if not isinstance(apri, ApriInfo):
            raise TypeError("`apri` must be of type `ApriInfo`.")

        if not isinstance(diskonly, bool):
            raise TypeError("`diskonly` must be of type `bool`.")

        if not isinstance(recursively, bool):
            raise TypeError("`recursively` must be of type `bool`.")

        try:
            return sum(t[1] for t in self.intervals(apri, combine = True, diskonly = diskonly, recursively = recursively))

        except DataNotFoundError:
            return 0

    def numBlks(self, apri, diskonly = False, recursively = False):

        self._checkOpenRaise("numBlks")

        if not isinstance(apri, ApriInfo):
            raise TypeError("`apri` must be of type `ApriInfo`.")

        if not isinstance(diskonly, bool):
            raise TypeError("`diskonly` must be of type `bool`.")

        if not isinstance(recursively, bool):
            raise TypeError("`recursively` must be of type `bool`.")

        try:
            self._checkKnownApri(apri)

        except DataNotFoundError:

            if not recursively:
                return 0

        else:

            with self._db.begin() as txn:
                ret = self._numDiskBlksTxn(apri, txn)

            if not diskonly:
                ret += sum(len(val) for val in self._ramBlks.values())

        if recursively:

            for subreg in self._iterSubregs():

                with subreg._recursiveOpen(True) as subreg:
                    ret += subreg.numBlks(apri, diskonly, True)


        return ret

    def maxn(self, apri, diskonly = False, recursively = False):

        self._checkOpenRaise("maxn")

        if not isinstance(apri, ApriInfo):
            raise TypeError("`apri` must be of type `ApriInfo`.")

        if not isinstance(diskonly, bool):
            raise TypeError("`diskonly` must be of type `bool`.")

        if not isinstance(recursively, bool):
            raise TypeError("`recursively` must be of type `bool`.")

        ret = -1

        try:
            self._checkKnownApri(apri)

        except DataNotFoundError:

            if not recursively:
                raise

        else:

            for startn, length in self.intervals(apri, sort = False, combine = False, diskonly = diskonly, recursively = recursively):
                ret = startn + length - 1

        if recursively:

            for subreg in self._iterSubregs():

                with subreg._recursiveOpen(True) as subreg:

                    try:
                        ret = max(ret, subreg.maxn(apri, diskonly = diskonly, recursively = True))

                    except DataNotFoundError:
                        pass

        if ret == -1:
            raise DataNotFoundError(_blkNotFoundErrMsg(diskonly, apri))

        else:
            return ret

    #################################
    # PROTEC RAM & DISK BLK METHODS #

    def _resolveStartnLength(self, apri, startn, length, diskonly, txn = None):
        """
        :param apri: (type `ApriInfo`)
        :param startn: (type `int` or `NoneType`) Non-negative.
        :param length: (type `int` or `NoneType`) Positive.
        :raise DataNotFoundError: If `apri` is not known to this register, or if no data is found matching startn and
        length.
        :raise ValueError: If `startn_ is None and length_ is not None`.
        :return: (type `int`) Resolved `startn`, always `int`.
        :return: (type `int`) Resolved `length`, always `int`.
        """

        if startn is None and length is not None:
            raise ValueError(f"If you specify a `Block` length, you must also specify a `startn`.")

        elif startn is not None and length is not None:
            return startn, length

        if not diskonly and apri in self._ramBlks.keys():

            if startn is not None and length is None:

                for blk in self._ramBlks[apri]:

                    if blk.startn() == startn:
                        return startn, len(blk)

            else:
                return self._ramBlks[apri][0].startn(), len(self._ramBlks[apri][0])

        if startn is not None and length is None:

            key = self._getDiskBlkKey(_BLK_KEY_PREFIX, apri, None, startn, 1, False, txn) # raises DataNotFoundError
            first_key_sep_index = key.find(_KEY_SEP)
            second_key_sep_index = key.find(_KEY_SEP, first_key_sep_index + 1)
            prefix = key [ : second_key_sep_index + 1]

            with lmdbPrefixIter(txn if txn is not None else self._db, prefix) as it:

                for key, _ in it:
                    return self._convertDiskBlockKey(_BLK_KEY_PREFIX_LEN, key, apri, None)[1:]

                else:
                    raise DataNotFoundError(
                        _blkNotFoundErrMsg(True, apri, None, startn, None)
                    )

        else:

            prefix = _BLK_KEY_PREFIX + self._getIdByApri(apri, None, False) + _KEY_SEP

            with lmdbPrefixIter(self._db, prefix) as it:

                for key, _ in it:
                    return self._convertDiskBlockKey(_BLK_KEY_PREFIX_LEN, key, apri, None)[1:]

                else:
                    raise DataNotFoundError(_blkNotFoundErrMsg(True, apri))

class PickleRegister(Register):

    @classmethod
    def withSuffix(cls, filename):
        return filename.with_suffix(".pkl")

    @classmethod
    def dumpDiskData(cls, data, filename, **kwargs):

        if len(kwargs) > 0:
            raise KeyError("This method accepts no keyword-arguments.")

        with filename.open("wb") as fh:
            pickle.dump(data, fh)

    @classmethod
    def loadDiskData(cls, filename, **kwargs):

        if len(kwargs) > 0:
            raise KeyError("`Pickle_Register.blk` accepts no keyword-arguments.")

        with filename.open("rb") as fh:
            return pickle.load(fh), filename

Register.addSubclass(PickleRegister)

class NumpyRegister(Register):

    @classmethod
    def dumpDiskData(cls, data, filename, **kwargs):

        if len(kwargs) > 0:
            raise KeyError("This method accepts no keyword-arguments.")

        np.save(filename, data, allow_pickle = False, fix_imports = False)

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
                "The keyword-argument `mmap_mode` for `Numpy_Register.blk` can only have the values " +
                "`None`, 'r+', 'r', 'w+', 'c'. Please see " +
                "https://numpy.org/doc/stable/reference/generated/numpy.memmap.html#numpy.memmap for more information."
            )

        return np.load(filename, mmap_mode = mmap_mode, allow_pickle = False, fix_imports = False)

    @classmethod
    def cleanDiskData(cls, filename, **kwargs):

        if len(kwargs) > 0:
            raise KeyError("This method accepts no keyword-arguments.")

        filename = filename.with_suffix(".npy")
        return Register.cleanDiskData(filename)

    @classmethod
    def withSuffix(cls, filename):
        return filename.with_suffix(".npy")

    def blk(self, apri, startn = None, length = None, diskonly = False, recursively = False, retMetadata = False, **kwargs):
        """
        :param apri: (type `ApriInfo`)
        :param startn: (type `int`)
        :param length: (type `length_`) non-negative
        :param retMetadata: (type `bool`, default `False`) Whether to return a `File_Metadata` object, which
        contains file creation date/time and size of dumped saved on the disk.
        :param recursively: (type `bool`, default `False`) Search all subregisters for the `Block`.
        :param mmap_mode: (type `str`, optional) Load the Numpy file using memory mapping, see
        https://numpy.org/doc/stable/reference/generated/numpy.memmap.html#numpy.memmap for more information.
        :return: (type `File_Metadata`) If `retMetadata is True`.
        """

        ret = super().blk(apri, startn, length, diskonly, recursively, retMetadata,  **kwargs)

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

        startn, length = Register._checkApriStartnLengthRaise(apri, startn, length)

        if not isinstance(retMetadata, bool):
            raise TypeError("`retMetadata` must be of type `bool`.")

        self._checkKnownApri(apri)
        # infer startn
        startn, _ = self._resolveStartnLength(apri, startn, length, True)
        # this implementation depends on `intervals` returning smaller startn before larger
        # ones and, when ties occur, larger lengths before smaller ones.
        if length is None:
            # infer length

            current_segment = False
            length = 0

            for _startn, _length in self.intervals(apri, combine = False, diskonly = False):

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
                            raise DataNotFoundError(_blkNotFoundErrMsg(True, apri, None, startn))

                        elif startn == _startn:

                            length += _length
                            current_segment = True

            if length == 0:
                raise RuntimeError("could not infer a value for `length`.")

            warnings.warn(f"`length` value not specified, inferred value: `length = {length}`.")

        combined_interval = None

        last_check = False
        last__startn = None
        _startn = None
        _length = None
        intervals_to_get = []

        for _startn, _length in self.intervals(apri, combine = False, diskonly = True):
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
                            f"The first `Block` does not have the right size. Try again by calling "
                            f"`reg.concatDiskBlks({str(apri)}, {_startn}, {length - (_startn - startn)})`."
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
                                    f"The last `Block` does not have the right size. Try again by calling "
                                    f"`reg.concatDiskBlks({str(apri)}, {startn}, "
                                    f"{length - (_startn + _length - (startn + length))})`."
                                )

                            combined_interval = (startn, combined_interval[1] + _length)
                            intervals_to_get.append((_startn, _length))
                            last_check = _startn + _length == startn + length

                        else:
                            raise ValueError(f"Overlapping `Block` intervals found with {str(apri)}.")

        else:

            if _startn is None:
                raise DataNotFoundError(_blkNotFoundErrMsg(True, apri))

            elif _startn + _length != startn + length:
                raise ValueError(
                    f"The last `Block` does not have the right size. "
                    f"Try again by calling `reg.concatDiskBlks(apri, {startn}, {_startn + _length})`."
                )

        if len(intervals_to_get) == 1:

            if retMetadata:
                return self.blkMetadata(apri, *intervals_to_get)

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

                blk = self.blk(apri, _startn, _length, True, False, False, mmap_mode="r")
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
                    blk.release()
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
                blk.release()

        return ret

Register.addSubclass(NumpyRegister)

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

    def updateIntervalsCalculated(self):

        self.intervals = list(
            self.reg.intervals(self.apri, sort = True, combine = False, diskonly = self.diskonly, recursively = self.recursively)
        )

    def getNextBlk(self):

        if self.curr_blk is not None and isinstance(self.curr_blk, ReleaseBlock):
            self.curr_blk.release()

        return self.reg.blkByN(self.apri, self.curr_n, self.diskonly, self.recursively, False, **self.kwargs)

    def __iter__(self):
        return self

    def __next__(self):

        if self.stop is not None and self.curr_n >= self.stop:
            raise StopIteration

        elif self.curr_blk is None:

            self.updateIntervalsCalculated()

            if len(self.intervals) == 0:
                raise StopIteration

            self.curr_n = max(self.intervals[0][0], self.curr_n)

            try:
                self.curr_blk = self.getNextBlk()

            except DataNotFoundError:
                raise StopIteration

        elif self.curr_n not in self.curr_blk:

            try:
                self.curr_blk = self.getNextBlk()

            except DataNotFoundError:

                self.updateIntervalsCalculated()

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

class _CopyRegister(Register):

    @classmethod
    def withSuffix(cls, filename):
        return filename

    @classmethod
    def dumpDiskData(cls, data, filename, **kwargs):

        if not isinstance(data, Path):
            raise TypeError("`data` must of of type `Path`.")

        if not data.absolute():
            raise ValueError("`data` must be an absolute `Path`.")

        filename = filename.with_suffix(data.suffix)
        shutil.copyfile(data, filename)
        return filename

    @classmethod
    def loadDiskData(cls, filename, **kwargs):
        raise NotImplementedError

    def setClsName(self, clsName):

        with self._clsFilepath.open() as fh:
            fh.write(clsName)

# def updateRegVersion(ident):
#
#     from venv import EnvBuilder
#     from subprocess import check_call, DEVNULL
#     import importlib
#     import string
#     import re
#
#     ident = Path(ident)
#
#     if not ident.absolute():
#         ident = Path.cwd() / ident
#
#     checkRegStructure(ident)
#
#     with (ident / VERSION_FILEPATH).open("r") as fh:
#         oldVers = fh.read()
#
#     if oldVers in COMPATIBLE_VERSIONS:
#
#         print(f"Register version at {ident} is up-to-date.")
#         return
#
#     builder = EnvBuilder(with_pip = True)
#     print("Setting up temporary venv....")
#     envDir = randomUniqueFilename(Path.home())
#     oldCorniferName = None
#
#     try:
#
#         builder.create(envDir)
#         print("... done.")
#         print("Downloading old cornifer version....")
#         oldCorniferDir = randomUniqueFilename(Path.home(), length = 25)
#         oldCorniferDir.mkdir(exist_ok = False)
#         check_call(
#             ["git", "clone", "--depth", "1", "--branch", oldVers, "https://github.com/automorphis/cornifer.git", str(oldCorniferDir)],
#             stderr = DEVNULL, stdout = DEVNULL
#         )
#         print("... done.")
#         print("Installing old cornifer.... ")
#
#         # this name is not guaranteed to be a unique package name, but it will be with extremely high probability
#         oldCorniferName = (
#             "cornifer_" +
#             randomUniqueFilename(Path.home(), length = 25, alphabet = string.ascii_uppercase + string.ascii_lowercase).name
#         )
#
#         for filename in oldCorniferDir.glob("**/*.py"):
#
#             with filename.open("r") as fh:
#                 editedText = re.sub("cornifer", oldCorniferName, fh.read())
#
#             with filename.open("w") as fh:
#                 fh.write(editedText)
#
#         (oldCorniferDir / "lib" / "cornifer").rename(oldCorniferDir / "lib" / oldCorniferName)
#         check_call(
#             ["pip", "install", str(oldCorniferDir)],
#             stderr = DEVNULL, stdout = DEVNULL
#         )
#         importlib.invalidate_caches()
#         print("... done.")
#         print("Updating register....")
#
#         if oldVers in ["0.1.0", "0.2", "0.3"]:
#
#             oldRegLoader = importlib.import_module(".register_loader", oldCorniferName)
#             dbMapSize = 25 * BYTES_PER_MB
#
#         else:
#
#             with (ident / MAP_SIZE_FILEPATH).open("r") as fh:
#                 dbMapSize = int(fh.read())
#
#             oldRegLoader = importlib.import_module(".regloader", oldCorniferName)
#
#         with (ident / CLS_FILEPATH).open("r") as fh:
#             clsName = fh.read()
#
#         with (ident / MSG_FILEPATH).open("r") as fh:
#             msg = fh.read()
#
#         oldErrors = importlib.import_module(".errors", oldCorniferName)
#         newReg = _CopyRegister(ident.parent, msg, dbMapSize)
#         oldReg = oldRegLoader.load(ident)
#
#         with oldReg.open() as oldReg:
#
#             with newReg.open() as newReg:
#
#                 if oldVers in ["0.1.0", "0.2", "0.3"]:
#
#                     for apri in oldReg:
#
#                         for startn, length in oldReg.disk_intervals(apri):
#
#                             metadata = oldReg.get_disk_block_metadata(apri, startn, length)
#                             apri = ApriInfo.fromJson(apri.to_json())
#                             newReg.addDiskBlk(Block(blk.get_segment(), apri, blk.get_start_n()))
#
#                         try:
#                             apos = oldReg.get_apos_info()
#
#                         except oldErrors.Data_Not_Found_Error:
#                             pass
#
#                         else:
#                             newReg.setApos(apos)
#
#                 else:
#
#                     for apri in oldReg:
#
#                         for blk in oldReg.diskBlks(apri):
#
#                             apri = ApriInfo.fromJson(blk.apri().toJson())
#                             newReg.addDiskBlk(Block(blk.segment(), apri, blk.startn()))
#
#                         try:
#                             apos = oldReg.apos(apri)
#
#                         except oldErrors.DataNotFoundError:
#                             pass
#
#                         else:
#                             newReg.setApos(apos)
#
#         newReg.setClsName(clsName)
#         print("... done.")
#         print("Deleting old register....")
#         shutil.rmtree(ident)
#         newReg._localDir.rename(ident.name)
#         print("... done.")
#
#     except:
#
#         erroredOut = True
#         raise
#
#     else:
#         erroredOut = False
#
#     finally:
#
#         if erroredOut:
#             print("Encountered an error, cleaning up...")
#
#         if oldCorniferName is not None:
#
#             print("Uninstalling old cornifer....")
#             check_call(
#                 ["pip", "uninstall", oldCorniferName],
#                 stderr = DEVNULL, stdout = DEVNULL
#             )
#             print("... done.")
#
#         print("Removing temporary venv...")
#
#         if envDir.exists():
#             shutil.rmtree(envDir)
#
#         print("... done.")
#
#         if not erroredOut:
#             print("Update successful!")