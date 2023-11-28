import warnings
from contextlib import contextmanager

class _BaseTransaction:

    write = False

    def __init_subclass__(cls, **kwargs):

        cls.write = kwargs.pop('write')
        super().__init_subclass__(**kwargs)

    def __init__(self, db):

        self._rw_txn = None
        self._db = db
        self._committed = False

    @contextmanager
    def begin(self):

        with self._db.begin(write = type(self).write) as rw_txn:

            self._rw_txn = rw_txn
            yield self

        self._committed = True

    def put(self, key, val):

        if 8 + 6 + len(key) > 4096:
            warnings.warn(f"Long database key results in disk memory ineffiency:\n{key}")

        self._rw_txn.put(key, val)

    def get(self, key, default = None):
        return self._rw_txn.get(key, default = default)

    def delete(self, key):
        self._rw_txn.delete(key)

    def prefix_iter(self, prefix):

        prefix_len = len(prefix)

        with self._rw_txn.cursor() as cursor:

            if not cursor.set_range(prefix):
                return

            while True:

                key, val = cursor.item()

                if key[:prefix_len] != prefix:
                     return

                else:
                    yield key, val

                if not cursor.next():
                    return

    def count_keys(self, prefix):

        ret = 0

        for _ in self.prefix_iter(prefix):
            ret += 1

        return ret

    def has_key(self, key):
        return self._rw_txn.get(key, default = None) is not None

    def cast(self, cls):

        if not issubclass(type(self), cls):
            raise TypeError

        if self._rw_txn is None:
            raise ValueError

        self.__class__ = cls
        self.__init__(self._db)
        return self

class Reader(_BaseTransaction, write = False):

    def put(self, key, val):
        raise NotImplementedError

    def delete(self, key):
        raise NotImplementedError

class Writer(_BaseTransaction, write = True):
    pass

class ReversibleWriter(Writer):

    def __init__(self, db):

        self.undo = {}
        super().__init__(db)

    def reverse(self, rw_txn):

        if self._committed:

            for key, val in self.undo.items():

                if val is None:
                    rw_txn.delete(key)

                else:
                    rw_txn.put(key, val)

    def put(self, key, val):

        if key not in self.undo.keys():
            self.undo[key] = self._rw_txn.get(key, default = None)

        super().put(key, val)

    def delete(self, key):

        if key not in self.undo.keys():
            self.undo[key] = self._rw_txn.get(key, default = None)

        super().delete(key)

class StagingReader(Reader):

    def __init__(self, db):

        self.stage = {}
        self.key_order = []
        super().__init__(db)

    def put(self, key, val):

        for i, key_ in enumerate(self.key_order):

            if key < key_:

                self.key_order.insert(i, key)
                break

        else:
            self.key_order.append(key)

        self.stage[key] = val

    def delete(self, key):
        self.stage[key] = None

    def get(self, key, default = None):

        try:
            val = self.stage[key]

        except KeyError:
            return super().get(key, default)

        else:

            if val is None:
                return default

            else:
                return val

    def empty_state(self):
        return len(self.stage.keys()) == 0

    def commit_stage(self, rw_txn):

        for key, val in self.stage.items():

            if val is not None:
                rw_txn.put(key, val)

            else:
                rw_txn.delete(key)

    def prefix_iter(self, prefix):

        if len(self.key_order) > 0:

            current_stage_index = 0
            current_stage_key = self.key_order[0]

            for key, val in super().prefix_iter(prefix):

                if current_stage_key is not None and current_stage_key < key:

                    yield current_stage_key
                    current_stage_index += 1

                    if current_stage_index >= len(self.key_order):
                        current_stage_key = None

                    else:
                        current_stage_key = self.key_order[current_stage_index]

                else:
                    yield key

        else:
            yield from super().prefix_iter(prefix)

def db_has_key(key, db):
    """DEPRECATED, use `_BaseTransaction.has_key` instead."""
    with db.begin() as r_txn:
        return r_txn.has_key(key)

def db_prefix_list(prefix, db):
    """DEPRECATED, use list(_BaseTransaction.prefix_iter) instead."""
    with db.begin() as ro_txn:
        return list(ro_txn.prefix_iter(prefix))

@contextmanager
def db_prefix_iter(prefix, db):
    """DEPRECATED, use `_BaseTransaction.prefix_iter` instead."""
    with Reader(db).begin() as ro_txn:
        yield from ro_txn.prefix_iter(prefix)

def db_count_keys(prefix, db):
    """DEPRECATED, use `_BaseTransaction.count_keys` instead."""
    with Reader(db).begin() as r_txn:
        return r_txn.count_keys(prefix)