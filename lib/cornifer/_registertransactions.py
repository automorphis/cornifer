from abc import ABC, abstractmethod

from .blocks import Block
from ._utilities import check_type
from ._utilities.lmdb import ReversibleTransaction

class RegisterWriteMethod(ABC):

    def __init__(self, reg):
        self._reg = reg

    @abstractmethod
    def type_value(self):
        pass

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

class AddDiskBlk(RegisterWriteMethod):

    def __init__(self, reg, blk, exists_ok, dups_ok, ret_metadata):

        self._blk = blk
        self._exists_ok = exists_ok
        self._dups_ok = dups_ok
        self._ret_metadata = ret_metadata
        self._apri_json = self._blk_key = self._compressed_key = self._filename = self._add_apri = None
        super().__init__(reg)

    def type_value(self):

        self._reg._check_open_raise("add_disk_blk")
        self._reg._check_readwrite_raise("add_disk_blk")
        self._reg._check_blk_open_raise(self._blk, "add_disk_blk")
        check_type(self._blk, "blk", Block)
        check_type(self._exists_ok, "exists_ok", bool)
        check_type(self._dups_ok, "dups_ok", bool)
        check_type(self._ret_metadata, "ret_metadata", bool)

    def pre(self, r_txn):

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

    def disk(self, rrw_txn):
        pass

    def disk2(self):
        pass

    def error(self, rrw_txn, rw_txn, e):
        pass


class AppendDiskBlk(AddDiskBlk):
    pass

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

            with ReversibleTransaction(self._reg._db).begin() as rrw_txn:

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



