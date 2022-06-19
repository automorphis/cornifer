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
from pathlib import Path, PurePath

from cornifer.file_metadata import File_Metadata
from cornifer.errors import Register_Error
from cornifer.registers import Register, _BLK_KEY_PREFIX, _BLK_KEY_PREFIX_LEN
from cornifer.register_file_structure import REGISTER_LEVELDB_FILENAME, LOCAL_DIR_CHARS

_ARGS_TYPES = {
    "reg_limit" : int,

    "print_apri" : bool,
    "apri_limit" : int,

    "print_intervals" : bool,
    "print_interval_mode" : str,
    "interval_limit" : int,

    "print_incompatible_registers" : bool,

    "key_exact_match" : bool,

    "tuple_exact_match" : bool,

    "dict_exact_match" : bool,

    "str_exact_match" : bool,

}

_args = {
    "reg_limit" : 10,

    "print_apri" : True,
    "apri_limit" : 5,

    "print_intervals" : True,
    "print_interval_mode" : "combined", # combined, uncombined
    "interval_limit" : 5,

    "print_warnings_" : True,
    "warnings_limit" : 10,

    "print_incompatible_registers" : False,

    "key_exact_match" : False,

    "tuple_exact_match" : False,

    "dict_exact_match" : False,

    "str_exact_match" : False
}

def set_search_args(**kwargs):
    """This function changes the output of the `search` function."""

    for key,val in kwargs:

        if key not in _ARGS_TYPES.keys():
            raise KeyError(f"Unrecognized `search_arg` key: {key}")

        elif not isinstance(val, _ARGS_TYPES[key]):
            raise TypeError(f"Expected type for key \"{key}\" value : {_ARGS_TYPES[key].__name__}")

        elif _ARGS_TYPES[key] == int and val <= 0:
            raise ValueError(f"Value for key \"{key}\" must be a positive integer.")

        elif key == "print_intervals_mode" and val not in ["combined", "uncombined"]:
            raise ValueError('Value for key "print_intervals_mode" can be either "combined" or "uncombined".')

        _args[key] = val


def load(identifier, saves_directory = None):

    if not isinstance(identifier, str):
        raise TypeError("`identifier` must be a string.")

    if saves_directory is None:
        saves_directory = Path.cwd()

    elif isinstance(saves_directory, str):
        saves_directory = Path(saves_directory)

    elif not isinstance(saves_directory, PurePath):
        raise TypeError("`saves_directory` must be either `None`, a `pathlib.Path`, or a string.")

    try:
        resolved = saves_directory.resolve(True)

    except FileNotFoundError:
        raise_error = True

    else:
        raise_error = False

    if raise_error:
        resolved = saves_directory.resolve(False)
        for parent in reversed(resolved.parents):
            if not parent.exists():
                raise FileNotFoundError(
                    f"Resolved `saves_directory` : {resolved}\n" +
                    f"The file or directory `{str(parent)}` could not be found."
                )
        else:
            raise FileNotFoundError(f"The file or directory `{str(saves_directory)}` could not be found.")

    if "(" in identifier or ")" in identifier:
        raise ValueError("You don't need to include the parentheses for the `identifier` when you call `load`.")

    bad_symbs = [symb for symb in identifier if symb not in LOCAL_DIR_CHARS]
    if len(bad_symbs) > 0:
        raise ValueError("An identifier cannot contain any of the following symbols: " + "".join(bad_symbs))

    return Register._from_local_dir(saves_directory / identifier)

def search(apri = None, saves_directory = None, **kwargs):

    # Search happens in 3 phases:
    # 1. Test to make sure parameters have the correct types.
    # 2. Iterate over all `Register`s located in `saves_directory` and do each of the following three subphases on each
    #    `Register`:
    #    2a. Test to make sure the `Register` has a compatible version and load it.
    #    2b. Create two dictionaries `combined` and `uncombined`, whose keys are tuples of all registers and their
    #    corresponding apris. The values of `combined` are the return values of
    #    `reg.get_all_intervals(apri, combine = True)` and those of `uncombined` are the return values of
    #    `reg.get_disk_block_intervals(apri)`.
    #    2c. Apply the search parameters to obtain a `list` of `relevant` registers and apris.
    # 3. Print out descriptions of registers, apris, and blocks matching search criteria.

    ####################
    #     PHASE 1      #

    if saves_directory is None:
        saves_directory = Path.cwd()

    elif not isinstance(saves_directory, (Path, str)):
        raise TypeError("`saves_directory` must be either a string or of type `pathlib.Path`.")

    saves_directory = Path(saves_directory)

    # test that kwargs are hashable
    for key, val in kwargs.items():

        try:
            hash(val)

        except TypeError:
            raise TypeError(
                f"All search keyword arguments must be hashable types. The argument corresponding " +
                f"to the key \"{key}\" has type `{type(val)}`."
            )

    # convert kwargs keys to regular expressions
    key_res = [re.compile(key) for key in kwargs.keys()]

    ####################
    #     PHASE 2      #

    combined = {}
    uncombined = {}
    warnings_ = []
    relevant = []
    for local_dir in saves_directory.iterdir():

        leveldb_path = local_dir / REGISTER_LEVELDB_FILENAME
        if local_dir.is_dir() and leveldb_path.is_dir():

            ####################
            #     PHASE 2a     #

            # test if compatible register
            try:
                is_compatible_version = Register._is_compatible_version(local_dir)

            except FileNotFoundError:
                warnings_.append(f"`Register` at `{str(local_dir)}` does not have a version file.")
                continue

            if not is_compatible_version:
                if _args["print_incompatible_registers"]:
                    warnings_.append(f"`Register` at `{str(local_dir)}` has an incompatible version.")
                else:
                    continue

            # load register
            try:
                reg = Register._from_local_dir(local_dir)

            except (Register_Error, TypeError) as m:
                warnings_.append(f"`Register` at `{str(local_dir)}` not loaded. Error text: {str(m)}")
                continue

            ####################
            #     PHASE 2b     #

            encountered_error = False

            with reg.open() as reg:

                apris = reg.get_all_apri_info()

                for _apri in apris:

                    uncombined[reg, _apri] = reg.get_disk_block_intervals(_apri)
                    combined[reg, _apri] = reg.get_all_intervals(_apri, True, False)

            if encountered_error:
                continue


                # mode = _args["print_interval_mode"]

                # if mode == "disjoint_intervals":
                #     pass
                #
                # elif mode == "block_intervals":
                #     pass
                #
                # elif mode == "block_intervals_verbose":
                #     pass
                #
                # elif mode == "none":
                #     pass
                #
                # else:
                #     raise ValueError(f"unrecognized search argument: print_block_mode : {mode}")

            ####################
            #     PHASE 2c     #

            if apri is not None and apri in apris:
                # if the passed `apri` matches ANY of `apris`
                relevant.append((reg, apri))

            elif len(kwargs) > 0:

                for _apri in apris:
                    # find all `_apri` matching ALL the search criteria
                    for (key, val), key_re in zip(kwargs.items(), key_res):
                        # iterate over user's search critera
                        for _key, _val in _apri.__dict__:
                            # iterate over `_apri` data
                            if (
                                _key not in _apri._reserved_kws and (
                                    (key == _key and _args["key_exact_match"]) or
                                    (key_re.match(_key) is not None and not _args["key_exact_match"])
                                ) and
                                _val_match(val, _val)
                            ):
                                # found match, move on to next search criteria
                                break
                        else:
                            # if search criteria does not match `_apri`, then move on to next `_apri`
                            break

                    else:
                        # append iff the `else: break` clause is missed
                        relevant.append((reg, _apri))

            elif apri is None and len(kwargs) == 0:
                # if no search criteria given, then append all apri
                for _apri in apris:
                    relevant.append((reg, _apri))

    ####################
    #     PHASE 3      #

    prnt = ""

    if _args["print_warnings_"] and len(warnings_) > 0 and _args["warnings_limit"] > 0:

        prnt += "WARNINGS:\n"

        for i, w in enumerate(warnings_):

            if i >= _args["warnings_limit"]:
                prnt += f"... and {len(warnings_) - i} more.\n"
                break

            prnt += f"({i}) {w}\n"

        prnt += "\n"

    relevant = sorted(relevant, key = lambda t: t[0]._local_dir)
    current_reg = None
    reg_index = 0
    apri_index = 0
    hit_apri_limit = False

    prnt += "REGISTERS:\n"
    for reg,apri in relevant:

        if current_reg is None or current_reg != reg:

            current_reg = reg
            prnt += f"({reg._local_dir.name}) \"{str(reg)}\"\n"
            hit_apri_limit = False
            apri_index = 0

            if current_reg is not None:
                reg_index += 1

            else:
                reg_index = 0

        if reg_index >= _args["reg_limit"]:

            num_regs = len(set(_reg for _reg, _ in relevant))
            prnt += f"... and {num_regs - reg_index} more.\n"
            break

        if _args["print_apri"] and not hit_apri_limit:

            if apri_index >= _args["apri_limit"]:

                hit_apri_limit = True
                num_apri = len(set(_apri for _reg,_apri in relevant if _reg == reg))
                prnt += f"... and {num_apri - apri_index} more.\n"

            else:

                prnt += f"\t{repr(apri)}\n"

                if _args["print_intervals"]:

                    lim = _args["interval_limit"]

                    if _args["print_interval_mode"] == "combined":
                        ints = combined[reg, apri]

                    else:
                        ints = uncombined[reg, apri]

                    if len(ints) > 0:
                        prnt += f"\t\t{str(ints[:lim])[1:-1]}"

                        if lim > len(ints):
                            prnt += f" ... and {lim - len(ints)} more."

                        prnt += "\n."

                    else:
                        prnt += "\t\t<no intervals found>\n"

        apri_index += 1

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
        