"""
    Cornifer, an easy-to-use data manager for computational and experimental mathematics.
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

import re
from itertools import product
from pathlib import Path, PurePath

from cornifer.errors import Register_Error, Register_Not_Created_Error
from cornifer.registers import _REGISTER_LEVELDB_NAME, _CLS_KEY, Register, _MSG_KEY

_regs = []
_search_called = False

_ARGS_TYPES = {
    "reg_limit" : int,

    "print_apri" : bool,
    "apri_limit" : int,

    "print_intervals" : bool,
    "interval_limit" : int,

    "print_warnings" : bool,
    "warnings_limit" : int,

    "key_exact_match" : bool,

    "tuple_exact_match" : bool,

    "dict_exact_match" : bool,

    "str_exact_match" : bool
}
_args = {
    "reg_limit" : 10,

    "print_apri" : True,
    "apri_limit" : 5,

    "print_intervals" : True,
    "interval_limit" : 5,

    "print_warnings" : True,
    "warnings_limit" : 10,

    "key_exact_match" : False,

    "tuple_exact_match" : False,

    "dict_exact_match" : False,

    "str_exact_match" : False
}
def search_args(**kwargs):

    for key,val in kwargs:

        if key not in _ARGS_TYPES.keys():
            raise KeyError(f"Unrecognized `search_arg` key: {key}")
        elif not isinstance(val, _ARGS_TYPES[key]):
            raise TypeError(f"Expected type for key \"{key}\" value : {_ARGS_TYPES[key].__name__}")
        elif _ARGS_TYPES[key] == int and val < 0:
            raise ValueError(f"Value for key \"{key}\" must be a nonnegative integer.")

        _args[key] = val


def load(identifier, saves_directory = None):

    if not isinstance(identifier, str):
        raise TypeError("`ident` must be a `str`")

    if saves_directory is None:
        saves_directory = Path.cwd()
    elif isinstance(saves_directory, str):
        saves_directory = Path(saves_directory)
    elif not isinstance(saves_directory, PurePath):
        raise TypeError("if `saves_directory is not None`, then it must be either a `PurePath` or a `str`")

    try:
        return Register._from_local_dir(saves_directory / identifier)
    except Register_Not_Created_Error:
        raise Register_Not_Created_Error("load")

def search(apri = None, saves_directory = None, **kwargs):

    if saves_directory is None:
        saves_directory = Path.cwd()
    saves_directory = Path(saves_directory)

    for key, val in kwargs.items():
        try:
            hash(val)
        except TypeError:
            raise TypeError(
                f"All search keyword arguments must be hashable types. The argument corresponding " +
                f"to the key \"{key}\" has type `{type(val)}`."
            )

    kwargs = {
        key :
        re.compile(val) if isinstance(val,str) and _args["str_exact_match"]
        else val
        for key, val in kwargs.items()
    }

    warnings = []
    regs = []
    for d in saves_directory.iterdir():
        leveldb_path = d / _REGISTER_LEVELDB_NAME
        if d.is_dir() and leveldb_path.is_dir():

            try:
                reg = Register._from_local_dir(d)
            except (Register_Error, TypeError) as m:
                warnings.append(f"`Register` at `{d}` not loaded. Error text: {str(m)}")
                continue

            added = False
            with reg.open() as reg:
                apris = reg.get_all_apri_info()
                if _args["print_intervals"]:
                    ints = reg.list_intervals_calculated(apri)
                else:
                    ints = None

            app = (reg, apris, ints)
            if apri is not None:
                if apri in apris:
                    added = True
                    regs.append(app)

            if not added and len(kwargs) > 0:
                for (key, val), apri in product(kwargs.items(), apris):
                    key_re = re.compile(key)
                    for _key,_val in apri.__dict__:
                        if (
                            _key not in apri._reserved_kws and (
                                (key == _key and _args["key_exact_match"]) or
                                (key_re.match(_key) is not None and not _args["key_exact_match"])
                            ) and
                            _val_match(val, _val)
                        ):
                            regs.append(app)
                            break
                    else:
                        continue
                    break

            if apri is None and len(kwargs) == 0:
                regs.append(app)

    prnt = ""

    if _args["print_warnings"] and len(warnings) > 0:
        prnt += "WARNINGS:\n"
        for i, w in enumerate(warnings):
            if i >= _args["warnings_limit"]:
                break
            prnt += f"({i}) {w}\n"

    prnt += "REGISTERS:\n"
    for i, (reg,apris,ints) in enumerate(regs):
        if i >= _args["reg_limit"]:
            break
        prnt += f"({i}) {reg._local_dir.name} \"{str(reg)}\"\n"
        if _args["print_apri"]:
            for j,apri in enumerate(apris):
                if j >= _args["apri_limit"]:
                    break
                prnt += f"\t{repr(apri)}\n"
                if _args["print_intervals"]:
                    lim = _args["interval_limit"]
                    if len(ints) > 0:
                        prnt += f"\t\t{ints[:lim]}\n"
                    else:
                        prnt += "\t\t<no intervals calculated>\n"

    global _regs, _search_called
    _search_called = True
    _regs = regs

def _val_match(search_val, apri_val):

    if type(search_val) != type(apri_val):
        return False

    if isinstance(search_val, str):
        if _args["str_exact_match"]:
            search_val = re.compile(search_val)
        else:
            return search_val == apri_val

    if isinstance(search_val, re.Pattern):
        return search_val.match(apri_val) is not None

    elif isinstance(search_val, dict):
        if _args["dict_exact_match"]:
            return search_val == apri_val
        else:
            return all(val == apri_val[key] for key, val in search_val.items())

    elif isinstance(search_val, tuple):
        if _args["tuple_exact_match"]:
            return search_val == apri_val
        else:
            return search_val in apri_val
    else:
        return search_val == apri_val
        