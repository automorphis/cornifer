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

import random
import re
import string
from collections import OrderedDict
from datetime import datetime, timedelta, timezone
from pathlib import Path

import numpy as np

class FinalYield(Exception): pass

BYTES_PER_KB = 1024
BYTES_PER_MB = 1024**2
BYTES_PER_GB = 1024**3
BYTES_PER_CHAR = 1
# BASE52 are distinguishable ascii characters (e.g. 1, l are easily confused in some typefaces)
BASE52 = "2346789abcdefghijmnpqrstuvwxyzABCDEFGHJLMNPQRTUVWXYZ"
BASE94 = string.ascii_letters + string.digits + string.punctuation
_TXT_FILE_WARNING_MESSAGE = "DO NOT EDIT THIS FILE.\n"
_TXT_FILE_WARNING_MESSAGE_LEN = len(_TXT_FILE_WARNING_MESSAGE)
NAME_REGEX = re.compile("[_a-zA-Z]\w*")

try:
    LOCAL_TIMEZONE = datetime.now(timezone(timedelta(0))).astimezone().tzinfo

except RuntimeError:
    LOCAL_TIMEZONE = timezone.utc

def intervals_overlap(int1, int2):
    """Check if the half-open interval [int1[0], int1[0] + int1[1]) has a non-empty intersection with
    [int2[0], int2[0] + int2[1])"""

    if int1[1] == 0 or int2[1] == 0:
        return False
    if int1[1] < 0 or int2[1] < 0:
        raise ValueError

    a1,l1 = int1
    a2,l2 = int2
    return a1 <= a2 < a1 + l1 or a1 < a2 + l2 <= a1 + l1 or a2 <= a1 < a2 + l2 or a2 < a1 + l1 <= a2 + l2

def intervals_subset(int1, int2):
    return int2[0] <= int1[0] and int1[0] + int1[1] <= int2[0] + int2[1]

def random_unique_filename(directory, suffix ="", length = 6, alphabet = BASE52, num_attempts = 10):

    directory = Path(directory)

    for n in range(num_attempts):

        filename =  directory / "".join(random.choices(alphabet, k=length + n))

        if suffix != "":
            filename = filename.with_suffix(suffix)

        if not filename.exists():
            return filename

    raise RuntimeError("buy a lottery ticket fr")

def check_has_method(instance, method_name):
    return hasattr(instance.__class__, method_name) and callable(getattr(instance.__class__, method_name))

def write_txt_file(txt, file, overwrite = False):

    with file.open("w" if overwrite else "x") as fh:
        fh.write(_TXT_FILE_WARNING_MESSAGE + txt)

def read_txt_file(file):

    with file.open("r") as fh:
        return fh.read()[_TXT_FILE_WARNING_MESSAGE_LEN : ]

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

def sort_intervals(intervals):
    return sorted(list(intervals), key = lambda t: (t[0], -t[1]))

def combine_intervals(intervals_sorted):

    yield_ = None

    for startn, length in intervals_sorted:

        if yield_ is None:
            yield_ = (startn, length)

        elif startn <= yield_[0] + yield_[1]:
            yield_ = (yield_[0], max(startn + length - yield_[0], yield_[1]))

        else:

            yield yield_
            yield_ = (startn, length)

    if yield_ is not None:
        yield yield_

def replace_lists_with_tuples(obj):

    if isinstance(obj, dict):
        return {key: replace_lists_with_tuples(val) for key, val in obj.items()}

    elif isinstance(obj, list) or isinstance(obj, tuple):
        return tuple([replace_lists_with_tuples(x) for x in obj])

    else:
        return obj

def replace_tuples_with_lists(obj):

    if isinstance(obj, dict):
        return {key: replace_tuples_with_lists(val) for key, val in obj.items()}

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

        ordered_items = sorted(list(json_obj.items()), key=lambda t: t[0])
        return OrderedDict([
            (key, order_json_obj(val))
            for key, val in ordered_items
        ])

    elif isinstance(json_obj, list):
        return list(map(order_json_obj, json_obj))

    else:
        return json_obj

def is_signed_int(num):
    return isinstance(num, (int, np.int8, np.int16, np.int32, np.int64))

def is_int(num):
    return isinstance(num, (int, np.int8, np.int16, np.int32, np.int64, np.uint8, np.uint16, np.uint32, np.uint64))

def bytify_int(num, num_zeros = None):

    if num_zeros is None:
        return str(num).encode("ASCII")

    else:
        return f"{num:0{num_zeros}d}".encode("ASCII")

def intify_bytes(bytes_):
    return int(bytes_.decode("ASCII"))

def check_type(obj, name, expected_type):

    if not isinstance(obj, expected_type):
        raise TypeError(f"`{name}` must be of type `{expected_type.__name__}`, not `{type(obj).__name__}`.")

def check_type_None_default(obj, name, expected_type, default):

    if obj is not None and not isinstance(obj, expected_type):
        raise TypeError(f"`{name}` must be of type `{expected_type.__name__}`, not `{type(obj).__name__}`.")

    elif obj is not None:
        return obj

    else:
        return default

def check_return_int(obj, name):

    if not is_int(obj):
        raise TypeError(f"`{name}` must be of type `int`, not `{type(obj).__name__}`.")

    else:
        return int(obj)

def check_return_int_None_default(obj, name, default):

    if obj is not None and not is_int(obj):
        raise TypeError(f"`{name}` must be of type `int`, not `{type(obj).__name__}`.")

    elif obj is not None:
        return int(obj)

    else:
        return default

def check_Path(obj, name):

    if not isinstance(obj, (str, Path)):
        raise TypeError(f"`{name}` must be either of type `str` or `pathlib.Path`, not `{type(obj).__name__}`.")

def resolve_path(path):
    """
    :param path: (type `pathlib.Path`)
    :raise FileNotFoundError: If the path could not be resolved.
    :return: (type `pathlib.Path`) Resolved.
    """

    try:
        resolved = path.resolve(True) # True raises FileNotFoundError

    except FileNotFoundError:
        raise_error = True

    else:
        return resolved

    if raise_error:

        resolved = path.resolve(False) # False suppressed FileNotFoundError

        for parent in reversed(resolved.parents):

            if not parent.exists():
                raise FileNotFoundError(
                    f"Resolved path : `{resolved}`\n" +
                    f"The file or directory `{str(parent)}` could not be found."
                )

        else:
            raise FileNotFoundError(f"The file or directory `{path}` could not be found.")

def is_deletable(path):

    try:

        with path.open("a") as _: pass
        return True

    except OSError:
        return False

    except Exception as e:
        raise e

# def get_leftmost_layer(s, begin = 0):
#
#     if begin >= len(s):
#         return len(s), len(s)
#
#     symbs_re = re.compile("[()\[\]{}\"']")
#     opening_symbs = {"}" : "{", "]" : "[", ")" : "("}
#     non_escaped_single_quote = re.compile("(\\\\)*'")
#     non_escaped_double_quote = re.compile('(\\\\)*"')
#     stack = []
#
#     found_leftmost_layer = False
#     curr_symb_index = begin - 1
#
#     start_index = None
#
#     while not found_leftmost_layer:
#
#         if len(stack) > 0 and stack[-1] == "'":
#             string_end_index = non_escaped_single_quote.search(s, curr_symb_index + 1)
#
#         elif len(stack) > 0 and stack[-1] == '"':
#             string_end_index = non_escaped_double_quote.search(s, curr_symb_index + 1)
#
#         else:
#             string_end_index = -1
#
#         if string_end_index is None:
#             raise ValueError
#
#         elif string_end_index.start(0) >= 0:
#             stack.pop()
#             curr_symb_index = string_end_index.start(0)
#
#         else:
#             match = symbs_re.search(s, curr_symb_index + 1)
#
#             if match is None and len(stack) > 0:
#                 raise ValueError
#
#             elif match is not None:
#                 symb = match.group(0)
#                 symb_index = match.start(0)
#
#                 if symb in opening_symbs.keys():
#                     # if `symb` is a closing symbol
#
#                     if len(stack) == 0 or stack[-1] != opening_symbs[symb]:
#                         raise ValueError
#
#                     else:
#                         stack.pop()
#                         curr_symb_index = symb_index
#
#                 else:
#                     # if `symb` is an opening symbol or a single or double quote
#
#                     if len(stack) == 0:
#                         start_index = symb_index
#                     stack.append(symb)
#                     curr_symb_index = symb_index
#
#         found_leftmost_layer = len(stack) == 0
#
#     if start_index is None or curr_symb_index == -1:
#         return 0, len(s)-1
#
#     else:
#         return start_index, curr_symb_index