from abc import ABC, abstractmethod

from cornifer._utilities.lmdb import ReversibleTransaction


class RegisterWriteMethod(ABC):

    def __init__(self, *args, **kwargs):
        pass

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
    pass

class AppendDiskBlk(AddDiskBlk):
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



