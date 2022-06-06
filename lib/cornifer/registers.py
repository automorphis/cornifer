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

"""
TODO:
 - implement mmapping for Numpy_Register
 - fix `set_start_n_info` to signature `head, body_length, tail`
 - resolve leveldb header problem in sage
 - give public "metadata" methods a better name (x)
 - add version numbers to register databases (add to register loader) (x)
 - code Apos_Info (x)
 - check that saves_directory is string or pure_path instance to Register __init__ (x)
 - change `Data_Not_Found_Error` to not be `OSError` (x)
 - rename `Register` `msg` to `message` (x)
 - rename `Sequence` `data` to `segment` (x)
 - add type and value checks for all public methods (x)
 - convert `int` type checks using `is_int` method (x)
 - convert numpy ints to python ints (x)
 
 - complete test cases for recursive function calls
 - write test cases for search and load 
 - rework `test__from_name_same_register`
 - rework `test__from_local_dir_different_registers`

 - include docs for search_args
 - make docs about C++ build tools install problem on Windows 10
 - write test_docs
 - add subregister tutorial to docs
 - make docs about leveldb header problem in sage
"""

import inspect
import math
import pickle
from contextlib import contextmanager
from pathlib import Path, PurePath
from abc import ABC, abstractmethod

import numpy as np
import plyvel

from cornifer.errors import Subregister_Cycle_Error, Data_Not_Found_Error, \
    Data_Not_Dumped_Error, Register_Not_Open_Error, Register_Already_Open_Error, Register_Not_Created_Error, \
    Critical_Database_Error, Database_Error, Register_Error, Apri_Info_Not_Found_Error
from cornifer import Apri_Info, Block, Apos_Info
from cornifer.utilities import intervals_overlap, random_unique_filename, leveldb_has_key, \
    leveldb_prefix_iterator, is_int, BASE56
from cornifer.version import VERSION_FILE_NAME, CURRENT_VERSION, COMPATIBLE_VERSIONS

#################################
#         LEVELDB KEYS          #

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
_APOS_KEY_PREFIX         = b"apos"

_KEY_SEP_LEN             = len(_KEY_SEP)
_SUB_KEY_PREFIX_LEN      = len(_SUB_KEY_PREFIX)
_BLK_KEY_PREFIX_LEN      = len(_BLK_KEY_PREFIX)
_APRI_ID_KEY_PREFIX_LEN  = len(_APRI_ID_KEY_PREFIX)
_ID_APRI_KEY_PREFIX_LEN  = len(_ID_APRI_KEY_PREFIX)

_REGISTER_LEVELDB_NAME   = "register"

LOCAL_DIR_CHARS          = BASE56

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

        if Register._instance_exists(local_dir):
            # return the `Register` that has already been opened
            return Register._get_instance(local_dir)

        else:

            try:
                db = plyvel.DB(local_dir)
            except plyvel.IOError:
                raise Register_Already_Open_Error()
            except plyvel.Error:
                raise Register_Not_Created_Error("_from_local_dir")

            cls_name = db.get(_CLS_KEY)
            if cls_name is None:
                raise Register_Error(f"`{_CLS_KEY.decode('ASCII')}` key not found for register : {local_dir}")
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
                raise Register_Error(f"`{_MSG_KEY.decode('ASCII')}` key not found for register : {local_dir}")
            msg = msg_bytes.decode("ASCII")

            reg = con(local_dir.parent, msg)
            reg._set_local_dir(local_dir)
            return reg

    @staticmethod
    def _add_instance(local_dir, reg):
        Register._instances[local_dir] = reg

    @staticmethod
    def _instance_exists(local_dir):
        return local_dir in Register._instances.keys()

    @staticmethod
    def _get_instance(local_dir):
        return Register._instances[local_dir]

    #################################
    #    PUBLIC REGISTER METHODS    #

    def __eq__(self, other):

        if not self._created or not other._created:
            raise Register_Not_Created_Error("__eq__")

        elif type(self) != type(other):
            return False

        else:
            return self._local_dir.resolve() == other._local_dir.resolve()

    def __hash__(self):

        if not self._created:
            raise Register_Not_Created_Error("__hash__")

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
        """

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
            # set local directory info and create levelDB database
            local_dir = random_unique_filename(self.saves_directory, alphabet= LOCAL_DIR_CHARS)
            local_dir.mkdir()
            self._reg_file = local_dir / _REGISTER_LEVELDB_NAME
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
        if local_dir.parent.resolve() != self.saves_directory.resolve():
            raise ValueError(
                "The `local_dir` argument must be a sub-directory of `reg.saves_directory`.\n" +
                f"`local_dir.parent`    : {str(local_dir.parent)}\n"
                f"`reg.saves_directory` : {str(self.saves_directory)}"
            )
        if not local_dir.is_dir():
            raise FileNotFoundError(Register._LOCAL_DIR_ERROR_MSG)
        if not (local_dir / _REGISTER_LEVELDB_NAME).is_dir():
           raise FileNotFoundError(Register._LOCAL_DIR_ERROR_MSG)
        self._created = True
        self._local_dir = local_dir
        self._local_dir_bytes = str(self._local_dir).encode("ASCII")
        self._reg_file = self._local_dir / _REGISTER_LEVELDB_NAME
        self._subreg_bytes = (
            _SUB_KEY_PREFIX + self._local_dir_bytes
        )

    @staticmethod
    def _is_compatible_version(local_dir):

        with (local_dir / VERSION_FILE_NAME).open("r") as fh:
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

    #################################
    #      PROTEC APRI METHODS      #

    def _get_apri_by_id(self, _id):
        """Get JSON bytestring representing an `Apri_Info` instance.

        :param _id: (type `bytes`)
        :return: (type `bytes`)
        """
        return self._db.get(_ID_APRI_KEY_PREFIX + _id)

    def _get_id_by_apri(self, apri, apri_json, missing_ok):
        """Get an `Apri_Info` ID for this database. If `missing_ok is True`, then create an ID if the passed `apri` or
        `apri_json` is unknown to this `Register`.

        One of `apri` and `apri_json` can be `None`, but not both. If both are not `None`, then `apri` is used.

        `self._db` must be opened by the caller.

        :param apri: (type `Apri_Info`)
        :param apri_json: (type `bytes`)
        :param missing_ok: (type `bool`) Create an ID if the passed `apri` or `apri_json` is unknown to this `Register`.
        :raises ValueError: If both `apri` and `apri_json` are `None`.
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

        _id = self._db.get(key, default = None)

        if _id is not None:
            return _id

        elif missing_ok:
            _id = self._db.get(_CURR_ID_KEY)
            next_id = str(int(_id) + 1).encode("ASCII")
            with self._db.write_batch(transaction = True) as wb:
                wb.put(_CURR_ID_KEY, next_id)
                wb.put(key, _id)
                wb.put(_ID_APRI_KEY_PREFIX + _id, key[_APRI_ID_KEY_PREFIX_LEN:])
            return _id

        else:
            if apri is None:
                apri = Apri_Info.from_json(apri_json.decode("ASCII"))
            raise Apri_Info_Not_Found_Error(apri)

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

    def _get_apos_key(self, apri, apri_json, missing_ok):
        """Get a key for an `Apos_Info` entry.

        One of `apri` and `apri_json` can be `None`, but not both. If both are not `None`, then `apri` is used. If
        `missing_ok is True`, then create a new `Apri_Info` ID if one does not already exist for `apri`.

        :param apri: (type `Apri_Info`)
        :param apri_json: (type `bytes`)
        :param missing_ok: (type `bool`)
        :raises ValueError: If both `apri` and `apri_json` are `None`.
        :raises Apri_Info_Not_Found_Error: If `missing_ok is False` and `apri` is not known to this `Register`.
        :return: (type `bytes`)
        """

        if apri is None and apri_json is None:
            raise ValueError

        apri_id = self._get_id_by_apri(apri, apri_json, missing_ok)
        return _APOS_KEY_PREFIX + _KEY_SEP + apri_id

    #################################
    #  PUBLIC SUB-REGISTER METHODS  #

    def add_subregister(self, subreg):

        self._check_open_raise("add_subregister")

        if not isinstance(subreg, Register):
            raise TypeError("`subreg` must be of a `Register` derived type")

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

        if not isinstance(subreg, Register):
            raise TypeError("`subreg` must be of a `Register` derived type")

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
                subreg = Register._from_local_dir(filename)
                yield subreg

    def _get_subregister_key(self):
        return _SUB_KEY_PREFIX + self._local_dir_bytes

    #################################
    #    PUBLIC DISK BLK METHODS    #

    @classmethod
    @abstractmethod
    def dump_disk_data(cls, data, filename):
        """Dump data to the disk.

        This method should not change any properties of any `Register`, which is why it is a class-method and
        not an instance-method. It merely takes `data` and dumps it to disk.

        Most use-cases prefer the instance-method `add_disk_block`.

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
        """Dump a `Block` to disk and link it with this `Register`.

        :param blk: (type `Block`)
        """
        #TODO: throw error if key already exists

        self._check_open_raise("add_disk_block")

        if not isinstance(blk, Block):
            raise TypeError("`blk` must be of type `Block`.")

        start_n_head = blk.get_start_n() // self._start_n_tail_mod
        if start_n_head != self._start_n_head :
            raise IndexError(
                "The `start_n` for the passed `Block` does not have the correct head:\n" +
                f"`tail_length`   : {self._start_n_tail_length}\n" +
                f"expected `head` : {self._start_n_head}\n"
                f"`start_n`       : {blk.get_start_n()}\n" +
                f"`start_n` head  : {start_n_head}\n"
            )

        key = self._get_disk_block_key(blk.get_apri(), None, blk.get_start_n(), len(blk), True)
        if not leveldb_has_key(self._db, key):

            filename = random_unique_filename(self._local_dir)
            try:
                filename = type(self).dump_disk_data(blk.get_segment(), filename)
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

        if not length >= 0:
            raise ValueError("`length` must be non-negative")

        try:
            key = self._get_disk_block_key(apri, None, start_n, length, False)
            known_apri = True

        except Apri_Info_Not_Found_Error:
            key = None
            known_apri = False

        if known_apri and leveldb_has_key(self._db, key):
            filename = Path(self._db.get(key).decode("ASCII"))
            self._db.delete(key)
            filename.unlink() # TODO add error checks

    def get_disk_block(self, apri, start_n, length, recursively = False):

        self._check_open_raise("get_disk_block")

        if not isinstance(apri, Apri_Info):
            raise TypeError("`apri` must be of type `Apri_Info`.")

        if not is_int(start_n):
            raise TypeError("`start_n` must be of type `int`.")
        else:
            start_n = int(start_n)

        if not is_int(length):
            raise TypeError("`start_n` must be of type `int`.")
        else:
            length = int(length)

        if not isinstance(recursively, bool):
            raise TypeError("`recursively` must be of type `bool`.")

        if length < 0:
            raise ValueError("`length` must be non-negative")

        try:
            key = self._get_disk_block_key(apri, None, start_n, length, False)
            found_key = True
        except Apri_Info_Not_Found_Error:
            key = None
            found_key = False

        if found_key and leveldb_has_key(self._db, key):
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

        if not isinstance(apri, Apri_Info):
            raise TypeError("`apri` must be of type `Apri_Info`.")

        if not is_int(n):
            raise TypeError("`n` must be of type `int`.")
        else:
            n = int(n)

        if not isinstance(recursively, bool):
            raise TypeError("`recursively` must be of type `bool`.")

        if n < 0:
            raise ValueError("`n` must be positive")

        for apri, start_n, length, _ in self._iter_disk_block_metadatas(apri, None):
            if start_n <= n < start_n + length:
                return self.get_disk_block(apri, start_n, length)

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

        if not isinstance(apri, Apri_Info):
            raise TypeError("`apri` must be of type `Apri_Info`.")

        if not isinstance(recursively, bool):
            raise TypeError("`recursively` must be of type `bool`.")

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

    def _get_disk_block_key(self, apri, apri_json, start_n, length, missing_ok = False):
        """Get the database key for a disk `Block`.

        One of `apri` and `apri_json` can be `None`, but not both. If both are not `None`, then `apri` is used.
        `self._db` must be opened by the caller. This method only queries the database to obtain the `apri` ID.

        If `missing_ok is True` and an ID for `apri` does not already exist, then a new one will be created. If
        `missing_ok is False` and an ID does not already exist, then an error is raised.

        :param apri: (type `Apri_Info`)
        :param apri_json: (types `bytes`)
        :param start_n: (type `int`) The start index of the `Block`.
        :param length: (type `int`) The length of the `Block`.
        :raises ValueError: If both `apri` and `apri_json` are `None`.
        :raises Apri_Info_Not_Found_Error: If `missing_ok is False` and `apri` is not known to this `Register`.
        :return: (type `bytes`)
        """

        if apri is None and apri_json is None:
            raise ValueError

        _id = self._get_id_by_apri(apri, apri_json, missing_ok)
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
            prefix = _BLK_KEY_PREFIX + self._get_id_by_apri(apri,apri_json,False) #TODO: fix `_get_id_by_apri` call, add try-except clause

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

        raise Data_Not_Found_Error

    def get_ram_block_by_n(self, apri, n, recursively = False):

        if not isinstance(apri, Apri_Info):
            raise TypeError("`apri` must be of type `Apri_Info`.")

        if not is_int(n):
            raise TypeError("`n` must be of type `int`.")
        else:
            n = int(n)

        if not isinstance(recursively, bool):
            raise TypeError("`recursively` must be of type `bool`.")

        #TODO for warnings: if ram blocks have overlapping data, log warning when this methid
        # is called

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

        raise Data_Not_Found_Error

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
            isinstance(short, tuple) or
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
            for apri, start_n, length in self._iter_disk_block_metadatas(apri,None):
                if start_n <= n < start_n + length:
                    break
            else:
                raise Data_Not_Found_Error()
            return self.get_disk_block(apri, start_n, length, recursively)[n]

    def list_intervals_calculated(self, apri, recursively = False):

        if not isinstance(apri, Apri_Info):
            raise TypeError("`apri` must be of type `Apri_Info`.")

        if not isinstance(recursively, bool):
            raise TypeError("`recursively` must be of type `bool`.")

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