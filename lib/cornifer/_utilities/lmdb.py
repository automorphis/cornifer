from contextlib import contextmanager
from pathlib import Path

import lmdb

from cornifer._utilities import is_signed_int

def lmdb_is_closed(db):

    try:
        with db.begin() as _:
            pass

    except BaseException as e:

        if isinstance(e, lmdb.Error) and "Attempt to operate on closed/deleted/dropped object." in str(e):
            return True

        else:
            raise e

    else:
        return False

def open_lmdb(filepath, map_size, read_only):

    if not isinstance(filepath, Path):
        raise TypeError("`filepath` must be of type `pathlib.Path`.")

    if not is_signed_int(map_size):
        raise TypeError("`map_size` must be of type `int`.")
    else:
        map_size = int(map_size)

    if not isinstance(read_only, bool):
        raise TypeError("`read_only` must be of type `bool`.")

    if not filepath.is_absolute():
        raise ValueError("`filepath` must be absolute.")

    if map_size <= 0:
        raise ValueError("`map_size` must be positive.")

    return lmdb.open(
        str(filepath),
        map_size = map_size,
        subdir = True,
        readonly = read_only,
        create = False
    )

def lmdb_has_key(db_or_txn, key):
    """
    :param db_or_txn: If type `lmdb_cornifer.Environment`, open a new read-only transaction and close it after this function
    resolves. If type `lmdb_cornifer.Transaction`, do not close it after the function resolves.
    :param key: (type `bytes`)
    :return: (type `bool`)
    """

    with _resolve_db_or_txn(db_or_txn) as txn:
        return txn.get(key, default = None) is not None

def lmdb_prefix_list(db_or_txn, prefix):

    with lmdb_prefix_iterator(db_or_txn, prefix) as it:
        return [t for t in it]

@contextmanager
def lmdb_prefix_iterator(db_or_txn, prefix):
    """Iterate over all key-value pairs where they key begins with given prefix.

    :param db_or_txn: If type `lmdb_cornifer.Environment`, open a new read-only transaction and close it after this function
    resolves. If type `lmdb_cornifer.Transaction`, do not close it after the function resolves.
    :param prefix: (type `bytes`)
    :return: (type `_LMDB_Prefix_Iterator`)
    """

    with _resolve_db_or_txn(db_or_txn) as txn:

        it = _LMDB_Prefix_Iterator(txn, prefix)

        try:
            yield it

        finally:
            it.cursor.close()

def lmdb_count_keys(db_or_txn, prefix):

    count = 0

    with lmdb_prefix_iterator(db_or_txn, prefix) as it:

         for _ in it:
            count += 1

    return count

@contextmanager
def _resolve_db_or_txn(db_or_txn):

    if isinstance(db_or_txn, lmdb.Environment):

        if lmdb_is_closed(db_or_txn):
            raise lmdb.Error("Environment should not be closed.")

        txn = db_or_txn.begin()
        abort = True

    elif isinstance(db_or_txn, lmdb.Transaction):

        txn = db_or_txn
        abort = False

    else:
        raise TypeError

    try:
        yield txn

    finally:
        if abort:
            txn.abort()

class _LMDB_Prefix_Iterator:

    def __init__(self, txn, prefix):

        self.prefix = prefix
        self.prefix_len = len(prefix)
        self.cursor = txn.cursor()
        self.raise_stop_iteration = not self.cursor.set_range(prefix)

    def __iter__(self):
        return self

    def __next__(self):

        if self.raise_stop_iteration:
            raise StopIteration

        key, val = self.cursor.item()

        if key[ : self.prefix_len] != self.prefix:
            raise StopIteration

        else:
            self.raise_stop_iteration = not self.cursor.next()

        return key, val