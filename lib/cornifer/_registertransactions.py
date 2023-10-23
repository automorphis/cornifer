import warnings
from abc import ABC, abstractmethod

from . import DataNotFoundError, Register
from ._utilities import check_type, random_unique_filename, intervals_overlap
from ._utilities.lmdb import ReversibleWriter, r_txn_has_key
from .errors import DataExistsError


class RegisterWriteMethod(ABC):

    method_name = None

    def __init__(self, reg):
        self._reg = reg

    def __init_subclass__(cls, **kwargs):

        method_name = kwargs.pop("method_name", None)

        if method_name is None:
            raise ValueError

        cls.method_name = method_name
        super().__init_subclass__(kwargs)

    @abstractmethod
    def type_value(self):

        self._reg._check_open_raise(type(self).method_name)
        self._reg._check_readwrite_raise(type(self).method_name)

    @abstractmethod
    def pre(self, r_txn):
        pass

    @abstractmethod
    def disk(self, rrw_txn):
        pass

    @abstractmethod
    def disk2(self):
        pass

    @abstractmethod
    def error(self, rrw_txn, rw_txn, e):
        pass

class AddDiskBlk(RegisterWriteMethod, method_name = "add_disk_blk"):

    def __init__(self, reg, blk, apri_json, reencode, exists_ok, dups_ok, ret_metadata):

        self._blk = blk
        self._apri_json = apri_json
        self._reencode = reencode
        self._exists_ok = exists_ok
        self._dups_ok = dups_ok
        self._ret_metadata = ret_metadata
        self._blk_key = self._compressed_key = self._filename = self._add_apri = None
        super().__init__(reg)

    def type_value(self):

        super().type_value()
        self._reg._check_blk_open_raise(self._blk, type(self).method_name)
        check_type(self._exists_ok, "exists_ok", bool)
        check_type(self._dups_ok, "dups_ok", bool)
        check_type(self._ret_metadata, "ret_metadata", bool)

        if len(self._blk) > self._reg._max_length:
            raise ValueError

        startn_head = self._blk.startn() // self._reg._startn_tail_mod

        if startn_head != self._reg.startn_head:
            raise IndexError(
                "The `startn` for the passed `Block` does not have the correct head:\n"
                f"`tail_len`      : {self._reg._startn_tail_length}\n"
                f"expected `head` : {self._reg._startn_head}\n"
                f"`startn`        : {self._blk.startn()}\n"
                f"`startn` head   : {startn_head}\n"
                "Please see the method `set_startn_info` to troubleshoot this error."
            )

    def pre(self, r_txn):

        try:

            if self._reencode:
                self._apri_json = self._reg._relational_encode_info(self._blk.apri(), r_txn)

        except DataNotFoundError:
            self._add_apri = True

        else:

            apri_id_key = Register._get_apri_id_key(self._apri_json)
            self._add_apri = not Register._disk_apri_key_exists(apri_id_key, r_txn)

        self._filename = random_unique_filename(self._reg._local_dir, suffix = type(self._reg).file_suffix, length = 6)
        apri = self._blk.apri()
        startn = self._blk.startn()
        length = len(self._blk)

        if not self._add_apri:

            self._blk_key, self._compressed_key = self._reg._get_disk_blk_keys(
                apri, self._apri_json, False, startn, length, r_txn
            )

            if not self._exists_ok and r_txn_has_key(self._blk_key, r_txn):
                raise DataExistsError(
                    f"Duplicate `Block` with the following data already exists in this `Register`: {apri}, startn = "
                    f"{startn}, length = {length}."
                )

            if not self._dups_ok:

                prefix = self._reg._intervals_pre(apri, self._apri_json, False, r_txn)
                int1 = (startn, length)

                for int2 in self._reg._intervals_disk(prefix, r_txn):

                    if intervals_overlap(int1, int2):
                        raise DataExistsError(
                            "Attempted to add a `Block` with duplicate indices. Set `dups_ok` to `True` to suppress."
                        )

        else:
            self._blk_key = self._compressed_key = None

        if length == 0:
            warnings.warn(f"Added a length 0 disk `Block`.\n{apri}\nstartn = {startn}\n{self._reg}")

    def disk(self, rrw_txn):

        apri = self._blk.apri()

        if self._add_apri:

            self._reg._add_apri_disk(apri, [], False, rrw_txn)
            blk_key, compressed_key = self._reg._get_disk_blk_keys(apri, None, True, startn, length, rw_txn)

        filename_bytes = self._filename.name.encode("ASCII")
        rw_txn.put(blk_key, filename_bytes)
        rw_txn.put(compressed_key, _IS_NOT_COMPRESSED_VAL)

    def disk2(self):
        pass

    def error(self, rrw_txn, rw_txn, e):
        pass

class AppendDiskBlk(AddDiskBlk, method_name = "append_disk_blk"):

    def pre(self, r_txn):

        apri = self._blk.apri()
        startn = self._blk.startn()
        length = len(self._blk)

        try:

            if self._reencode:
                self._apri_json = self._reg._relational_encode_info(apri, r_txn)

        except DataNotFoundError:
            add_apri = True

        else:

            apri_id_key = Register._get_apri_id_key(self._apri_json)
            add_apri = not Register._disk_apri_key_exists(apri_id_key, r_txn)

        self._filename = random_unique_filename(self._reg._local_dir, suffix = type(self._reg).file_suffix, length = 6)

        if not add_apri:

            try:
                prefix = self._reg._maxn_pre(apri, self._apri_json, False, r_txn)

            except DataNotFoundError: # if apri has no disk blks (used passed startn)
                pass

            else:
                startn = self._reg._maxn_disk(prefix, r_txn) + 1

            self._blk_key, self._compressed_key = self._reg._get_disk_blk_keys(
                apri, self._apri_json, False, startn, length, r_txn
            )

        else:
            self._blk_key = self._compressed_key = None

        if length == 0:
            warnings.warn(f"Added a length 0 disk `Block`.\n{apri}\nstartn = {startn}\n{self._reg}")

class RmvDiskBlk(RegisterWriteMethod):
    pass

class Compress(RegisterWriteMethod):
    pass

class Decompress(RegisterWriteMethod):
    pass

class ChangeApri(RegisterWriteMethod):
    pass

class RmvApri(RegisterWriteMethod):
    pass

class SetMsg(RegisterWriteMethod):
    pass

class SetShorthand(RegisterWriteMethod):
    pass

class SetStartnInfo(RegisterWriteMethod):
    pass

class IncreaseSize(RegisterWriteMethod):
    pass

class AddSubreg(RegisterWriteMethod):
    pass

class RmvSubreg(RegisterWriteMethod):
    pass

class SetApos(RegisterWriteMethod):
    pass

class RmvApos(RegisterWriteMethod):
    pass

class RegisterTransaction:

    def __init__(self, reg):

        self._reg = reg
        self._methods = None

    def commit(self):

        for method in self._methods:
            method.type_value()

        with self._reg._db.begin() as ro_txn:

            for method in self._methods:
                method.pre(ro_txn)

        rrw_txn = None

        try:

            with ReversibleWriter(self._reg._db).begin() as rrw_txn:

                for method in self._methods:
                    method.disk(rrw_txn)

            for method in self._methods:
                method.disk2()

        except BaseException as e:

            if rrw_txn is not None:

                with self._reg._db.begin(write = True) as rw_txn:

                    for method in reversed(self._methods):
                        ee = method.error(rrw_txn, rw_txn, e)

                raise ee

            else:
                raise



