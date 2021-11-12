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
    """Check if the half-open interval [int1[0], int1[0] + int1[1]) has a non-empty intersection with
    [int2[0], int2[0] + int2[1])"""

    if int1[1] == 0 or int2[1] == 0:
        return False
    if int1[1] < 0 or int2[1] < 0:
        raise ValueError

    a1,l1 = int1
    a2,l2 = int2
    return a1 <= a2 < a1 + l1 or a1 < a2 + l2 <= a1 + l1 or a2 <= a1 < a2 + l2 or a2 < a1 + l1 <= a2 + l2

def log_raise_error(error, verbose, suppress_errors):
    if verbose:
        logging.warning(f"Error raised: {str(error)}")
        if suppress_errors:
            logging.warning("Error suppressed.")
    if not suppress_errors:
        raise error

def random_unique_filename(directory, suffix ="", length = 6, alphabet = BASE56, num_attempts = 10):
    directory = Path(directory)
    for _ in range(num_attempts):
        filename =  directory / "".join(random.choices(alphabet, k=length))
        if suffix:
            filename = filename.with_suffix(suffix)
        if not Path.is_file(filename):
            return filename
    raise RuntimeError("buy a lottery ticket fr")

def check_has_method(instance, method_name):
    return hasattr(instance.__class__, method_name) and callable(getattr(instance.__class__, method_name))

# def safe_overwrite_file(filename, new_content):
#     tempfile = random_unique_filename(filename.parent)
#     try:
#         with tempfile.open("w") as fh:
#             fh.write(new_content)
#         Path.unlink(filename)
#         Path.rename(tempfile, filename)
#     except OSError:
#         raise OSError(
#             "An error occured somewhere while updating data. A copy of the data can be found in either the "
#             f"file `{str(filename)}` or the file `{str(tempfile)}`."
#         )

def replace_lists_with_tuples(obj):
    if isinstance(obj, dict):
        return {key: replace_lists_with_tuples(val) for key,val in obj.items()}
    elif isinstance(obj, list) or isinstance(obj, tuple):
        return tuple([replace_lists_with_tuples(x) for x in obj])
    else:
        return obj

def replace_tuples_with_lists(obj):
    if isinstance(obj, dict):
        return {key: replace_tuples_with_lists(val) for key,val in obj.items()}
    elif isinstance(obj, tuple) or isinstance(obj, list):
        return [replace_tuples_with_lists(x) for x in obj]
    else:
        return obj

def justify_slice(slc, min_index, max_index):
    """If a slice has negative or `None` indices, then this function will return a new slice with equivalent,
    non-`None`, positive indices.

    :param slc: (type `slice`) The `slice` to justify.
    :param min_index: (type non-negative `int`) The minimum index of the justified slice.
    :param max_index: (type non-negative `int`) The maximum index of the justified slice.
    :return: The justified `slice`.
    """

    if max_index < min_index:
        raise ValueError("max_index < min_index")
    if max_index < 0:
        raise ValueError("max_index < 0")
    if min_index < 0:
        raise ValueError("min_index < 0")

    start = slc.start   if slc.start    else min_index
    stop =  slc.stop    if slc.stop     else max_index + 1
    step =  slc.step    if slc.step     else 1

    start = _justify_slice_start_stop(start, min_index, max_index)
    stop =  _justify_slice_start_stop(stop, min_index, max_index)

    return slice(start, stop, step)

def _justify_slice_start_stop(num, min_index, max_index):
    mod = max_index - min_index + 1
    if num < 0:
        num += mod + min_index
    if num < min_index:
        return 0
    elif num > max_index:
        return mod
    else:
        return num - min_index

def order_json_obj(json_obj):
    if isinstance(json_obj, dict):
        ordered_items = sorted(list(json_obj.items()),key=lambda t: t[0])
        return {
            key : order_json_obj(val)
            for key,val in ordered_items
        }
    elif isinstance(json_obj, list):
        return list(map(order_json_obj, json_obj))
    else:
        return json_obj

def leveldb_has_key(db, key):
    return db.get(key,default = None) is not None

@contextmanager
def leveldb_prefix_iterator(db, prefix):
    it = db.iterator(prefix=prefix)
    try:
        yield it
    finally:
        it.close()

def leveldb_count_keys(db, prefix):
    count = 0
    with leveldb_prefix_iterator(db, prefix) as it:
         for _ in it:
            count += 1
    return count
