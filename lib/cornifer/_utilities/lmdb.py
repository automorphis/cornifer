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

from contextlib import contextmanager
from pathlib import Path

import lmdb

from .._utilities import isInt

class ReversibleTransaction:

    def __init__(self, db):

        self.db = db
        self.txn = None
        self.errors = False
        self.committed = False
        self.undo = {}

    @contextmanager
    def begin(self, write = False):

        self.txn = self.db.begin(write = write)

        try:
            yield self

        except:

            self.errors = True
            raise

        finally:

            if self.errors:
                self.txn.abort()

            else:

                self.txn.commit()
                self.committed = True

    def cursor(self):
        return self.txn.cursor()

    def reverse(self, txn):

        if self.committed:

            for key, val in self.undo.items():

                if val is None:
                    txn.delete(key)

                else:
                    txn.put(key, val)

    def put(self, key, val):

        if key not in self.undo.keys():
            self.undo[key] = self.txn.get(key, default = None)

        self.txn.put(key, val)

    def get(self, key, default = None):
        return self.txn.get(key, default = default)

    def delete(self, key):

        if key not in self.undo.keys():
            self.undo[key] = self.txn.get(key, default = None)

        self.txn.delete(key)

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

    elif isinstance(dbOrTxn, (lmdb.Transaction, ReversibleTransaction)):

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