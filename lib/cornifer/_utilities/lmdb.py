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
import os
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path

import lmdb

from .._utilities import check_type, check_return_int


class ReversibleWriter:

    def __init__(self, db):


        self.db = db
        self.txn = None
        self.committed = False
        self.undo = {}

    @contextmanager
    def begin(self):

        with self.db.begin(write = True) as rw_txn:

            self.txn = rw_txn
            yield self

        self.committed = True

    def begin_no_cm(self):

        self.txn = self.db.begin(write = True)
        return self

    def commit(self):

        self.txn.commit()
        self.committed = True

    def abort(self):

        self.txn.abort()
        self.committed = False

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

def create_lmdb(filepath, mapsize, max_readers):

    check_type(filepath, "filepath", Path)
    mapsize = check_return_int(mapsize, 'mapsize')
    max_readers = check_return_int(max_readers, 'max_readers')

    if mapsize <= 0:
        raise ValueError

    if max_readers <= 0:
        raise ValueError

    return lmdb.open(
        str(filepath),
        map_size = mapsize,
        subdir = True,
        create = False,
        max_readers = max_readers
    )

def open_lmdb(filepath, mapsize, readonly):

    check_type(filepath, "filepath", Path)
    check_type(readonly, "readonly", bool)

    if not filepath.is_absolute():
        raise ValueError("`filepath` must be absolute.")

    return lmdb.open(
        str(filepath),
        map_size = mapsize,
        readonly = readonly
    )

def db_has_key(key, db):
    """DEPRECATED, use `r_txn_has_key` instead."""
    with db.begin() as r_txn:
        return r_txn_has_key(key, r_txn)

def r_txn_has_key(key, r_txn):
    return r_txn.get(key, default = None) is not None

def db_prefix_list(prefix, db):
    """DEPRECATED, use `r_txn_prefix_list` instead."""
    with db.begin() as r_txn:
        return r_txn_prefix_list(prefix, r_txn)

def r_txn_prefix_list(prefix, r_txn):

    with r_txn_prefix_iter(prefix, r_txn) as it:
        return list(it)

@contextmanager
def db_prefix_iter(prefix, db):
    """DEPRECATED, use `r_txn_prefix_iter` instead."""
    with db.begin() as r_txn:

        with r_txn_prefix_iter(prefix, r_txn) as it:
            yield it

@contextmanager
def r_txn_prefix_iter(prefix, r_txn):

    it = _LmdbPrefixIter(prefix, r_txn)

    try:
        yield it

    finally:
        it.cursor.close()

def db_count_keys(prefix, db):
    """DEPRECATED, use `r_txn_count_keys` instead."""
    with db.begin() as r_txn:
        return r_txn_count_keys(prefix, r_txn)

def r_txn_count_keys(prefix, r_txn):

    count = 0

    with r_txn_prefix_iter(prefix, r_txn) as it:

        for _ in it:
            count += 1

    return count

def num_open_readers_accurate(db):

    str_ = db.readers()

    if str_ == "(no active readers)\n":
        return 0

    else:
        return str_.count("\n") - 1 - str_.count("-")

def to_str(db):

    ret = ""

    with db_prefix_iter(b"", db) as it:

        for key, val in it:
            ret += key.decode("ASCII") + ", " + val.decode("ASCII") + "\n"

    if len(ret) > 0:
        return ret[:-1]

    else:
        return ""

def debug_lmdb_is_open(db):

    try:
        with db.begin(): pass

    except lmdb.Error:
        return False

    else:
        return True

def approx_memory(db):
    # use only for debugging
    stat = db.stat()
    current_size = stat['psize'] * (stat['leaf_pages'] + stat['branch_pages'] + stat['overflow_pages'] )

    with db.begin() as ro_txn:

        with r_txn_prefix_iter(b"", ro_txn) as it:
            entry_size_bytes = sum(len(key) + len(value) for key, value in it) * 1

    return current_size + entry_size_bytes

class _LmdbPrefixIter:

    def __init__(self, prefix, txn):

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