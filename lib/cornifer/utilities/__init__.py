"""
    Cornifer, an intuitive data manager for empirical mathematics
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

import logging
import random
from contextlib import contextmanager
from pathlib import Path

import plyvel

BYTES_PER_KB = 1024
BYTES_PER_MB = 1024**2
BYTES_PER_GB = 1024**3
BASE56 = "23456789abcdefghijkmnpqrstuvwxyzABCDEFGHJKLMNPQRSTUVWXYZ"

def intervals_overlap(int1,int2):
    a1,l1 = int1
    a2,l2 = int2
    return a1 <= a2 < a1 + l1 or a1 <= a2 + l2 < a1 + l1 or a2 <= a1 < a2 + l2 or a2 <= a1 + l1 < a2 + l2

def log_raise_error(error, verbose, suppress_errors):
    if verbose:
        logging.warning(f"Error raised: {str(error)}")
        if suppress_errors:
            logging.warning("Error suppressed.")
    if not suppress_errors:
        raise error

def random_unique_filename(directory, suffix ="", length = 20, alphabet = BASE56, num_attempts = 10):
    for _ in range(num_attempts):
        filename =  directory / "".join(random.choices(alphabet, k=length))
        if suffix:
            filename = filename.with_suffix(suffix)
        if not Path.is_file(filename):
            return filename
    raise RuntimeError("buy a lottery ticket fr")

def check_has_method(instance, method_name):
    return hasattr(instance.__class__, method_name) and callable(getattr(instance.__class__, method_name))

def safe_overwrite_file(filename, new_content):
    tempfile = random_unique_filename(filename.parent)
    try:
        with tempfile.open("w") as fh:
            fh.write(new_content)
        Path.unlink(filename)
        Path.rename(tempfile, filename)
    except OSError:
        raise OSError(
            "An error occured somewhere while updating data. A copy of the data can be found in either the "
            f"file `{str(filename)}` or the file `{str(tempfile)}`."
        )

def replace_lists_with_tuples(json_obj):
    if isinstance(json_obj, dict):
        return {key: replace_lists_with_tuples(val) for key,val in json_obj.items()}
    elif isinstance(json_obj, list):
        return tuple([replace_lists_with_tuples(x) for x in json_obj])
    else:
        return json_obj

def replace_tuples_with_lists(pre_json_obj):
    if isinstance(pre_json_obj, dict):
        return {key: replace_lists_with_tuples(val) for key,val in pre_json_obj.items()}
    elif isinstance(pre_json_obj, tuple):
        return [replace_lists_with_tuples(x) for x in pre_json_obj]
    else:
        return pre_json_obj

def justify_slice(slc, min_index, max_index, length):
    start = slc.start   if slc.start    else min_index
    stop =  slc.stop    if slc.stop     else max_index
    step =  slc.step    if slc.step     else 1

    start = _justify_slice_start_stop(start, min_index, max_index, length)
    stop =  _justify_slice_start_stop(stop,  min_index, max_index, length)

    return slice(start, stop, step)

def _justify_slice_start_stop(num, min_index, max_index, length):
    if num < 0:
        num += length
        if num < min_index:
            num = min_index
        elif num > max_index:
            num = max_index + 1
    elif num < min_index:
        num = min_index
    elif num > max_index:
        num = max_index + 1
    return num - min_index

@contextmanager
def open_leveldb(filename, create_if_missing = False):
    db = plyvel.DB(filename, create_if_missing=create_if_missing)
    try:
        yield db
    finally:
        db.close()

def leveldb_has_key(db, key):
    return db.get(key,default = None) is not None