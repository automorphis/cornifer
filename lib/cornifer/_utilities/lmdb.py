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
from pathlib import Path

import lmdb

from .._utilities import check_type, check_return_int

def create_lmdb(filepath, mapsize, max_readers):

    check_type(filepath, "filepath", Path)
    mapsize = check_return_int(mapsize, 'mapsize')
    max_readers = check_return_int(max_readers, 'max_readers')

    return lmdb.open(
        str(filepath),
        map_size = mapsize,
        subdir = True,
        create = False,
        max_readers = max_readers
    )

def open_lmdb(filepath, readonly):

    check_type(filepath, "filepath", Path)
    check_type(readonly, "readonly", bool)

    if not filepath.is_absolute():
        raise ValueError("`filepath` must be absolute.")

    return lmdb.open(str(filepath), readonly = readonly)

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