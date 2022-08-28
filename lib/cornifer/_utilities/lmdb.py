from contextlib import contextmanager
from pathlib import Path

import lmdb

from cornifer._utilities import isInt

def lmdbIsClosed(db):

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

def openLmdb(filepath, mapSize, readonly):

    if not isinstance(filepath, Path):
        raise TypeError("`filepath` must be of type `pathlib.Path`.")

    if not isInt(mapSize):
        raise TypeError("`map_size` must be of type `int`.")
    else:
        mapSize = int(mapSize)

    if not isinstance(readonly, bool):
        raise TypeError("`readonly` must be of type `bool`.")

    if not filepath.is_absolute():
        raise ValueError("`filepath` must be absolute.")

    if mapSize <= 0:
        raise ValueError("`map_size` must be positive.")

    return lmdb.open(
        str(filepath),
        map_size = mapSize,
        subdir = True,
        readonly = readonly,
        create = False
    )

def lmdbHasKey(dbOrTxn, key):
    """
    :param dbOrTxn: If type `lmdb_cornifer.Environment`, open a new read-only transaction and close it after this function
    resolves. If type `lmdb_cornifer.Transaction`, do not close it after the function resolves.
    :param key: (type `bytes`)
    :return: (type `bool`)
    """

    with _resolveDbOrTxn(dbOrTxn) as txn:
        return txn.get(key, default = None) is not None

def lmdbPrefixList(dbOrTxn, prefix):

    with lmdbPrefixIter(dbOrTxn, prefix) as it:
        return [t for t in it]

@contextmanager
def lmdbPrefixIter(dbOrTxn, prefix):
    """Iterate over all key-value pairs where they key begins with given prefix.

    :param dbOrTxn: If type `lmdb_cornifer.Environment`, open a new read-only transaction and close it after this function
    resolves. If type `lmdb_cornifer.Transaction`, do not close it after the function resolves.
    :param prefix: (type `bytes`)
    :return: (type `_LMDB_Prefix_Iterator`)
    """

    with _resolveDbOrTxn(dbOrTxn) as txn:

        it = _LmdbPrefixIter(txn, prefix)

        try:
            yield it

        finally:
            it.cursor.close()

def lmdbCountKeys(dbOrTxn, prefix):

    count = 0

    with lmdbPrefixIter(dbOrTxn, prefix) as it:

         for _ in it:
            count += 1

    return count

@contextmanager
def _resolveDbOrTxn(dbOrTxn):

    if isinstance(dbOrTxn, lmdb.Environment):

        if lmdbIsClosed(dbOrTxn):
            raise lmdb.Error("Environment should not be closed.")

        txn = dbOrTxn.begin()
        abort = True

    elif isinstance(dbOrTxn, lmdb.Transaction):

        txn = dbOrTxn
        abort = False

    else:
        raise TypeError

    try:
        yield txn

    finally:
        if abort:
            txn.abort()

class _LmdbPrefixIter:

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