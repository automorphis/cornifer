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
import time
import warnings
from pathlib import Path

from .errors import RegisterError, DataExistsError, DataNotFoundError
from .registers import Register
from .regfilestructure import LOCAL_DIR_CHARS, check_reg_structure, DIGEST_FILEPATH
from ._utilities import resolve_path, read_txt_file
from .version import CURRENT_VERSION, COMPATIBLE_VERSIONS

_ARGS_TYPES = {
    "reg_limit" : int,

    "print_apri" : bool,
    "apri_limit" : int,

    "print_intervals" : bool,
    "print_interval_mode" : str,
    "interval_limit" : int,

    "print_incompatible_regs" : bool,

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

    "print_incompatible_regs" : False,

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

def load_shorthand(shorthand, saves_dir = None, wait_for_latency = False, timeout = 60):

    if saves_dir is None:
        saves_dir = Path.cwd()

    else:

        if not isinstance(saves_dir, (str, Path)):
            raise TypeError("`saves_dir` must be a string or a `pathlib.Path`.")

        if not saves_dir.is_absolute():
            saves_dir = Path.cwd() / saves_dir

    start = time.time()

    while True:

        ret = None
        dirs = []

        for d in saves_dir.iterdir():

            dirs.append(d)

            if d.is_dir():

                try:
                    check_reg_structure(d)

                except FileNotFoundError:
                    pass

                else:

                    reg = Register._from_local_dir(d)

                    if reg.shorthand() == shorthand:

                        if ret is not None:
                            raise DataExistsError(
                                f"More than one `Register` found with shorthand `{shorthand}` (you can also use the "
                                f"function `load_ident` to load a `Register`) :\n{str(reg)}\n{str(ret)}\n{dirs}")

                        else:
                            ret = reg

        if ret is not None:

            if wait_for_latency:
                _wait_for_latency(ret.ident(), ret, timeout)

            return ret

        elif not wait_for_latency or time.time() - start >= timeout:
            raise DataNotFoundError(f"No `Register` found with shorthand `{shorthand}`.")

def load_ident(identifier, wait_for_latency = False, timeout = 60):

    if not isinstance(identifier, (str, Path)):
        raise TypeError("`ident` must be a string or a `pathlib.Path`.")

    identifier = Path(identifier)

    if not identifier.is_absolute():
        resolved = Path.cwd() / identifier

    else:
        resolved = identifier

    if "(" in resolved.name or ")" in resolved.name:
        raise ValueError("You don't need to include the parentheses for the `ident` when you call `load`.")

    bad_symbs = [symb for symb in resolved.name if symb not in LOCAL_DIR_CHARS]
    if len(bad_symbs) > 0:
        raise ValueError("An ident cannot contain any of the following symbols: " + "".join(bad_symbs))

    reg = Register._from_local_dir(resolved)

    if not reg._has_compatible_version():
        warnings.warn(
            f"The register at `{reg._local_dir}` has an incompatible version.\n"
            f"Current Cornifer version: {CURRENT_VERSION}\n"
            f"Compatible version(s):    {', '.join(COMPATIBLE_VERSIONS)}\n"
            f"Loaded register version:  {reg._version}"
        )

    return reg

def search(apri = None, saves_directory = None, **kwargs):

    # Search happens in 3 phases:
    # 1. Test to make sure parameters have the correct types.
    # 2. Iterate over all `Register`s located in `savesDir` and do each of the following three subphases on each
    #    `Register`:
    #    2a. Load the `Register` and check that it has a compatible version.
    #    2b. Create two dictionaries `combined` and `uncombined`, whose keys are tuples of all registers and their
    #    corresponding apris. The values of `combined` are the return values of
    #    `reg.get_all_intervals(info, combine = True)` and those of `uncombined` are the return values of
    #    `reg.get_disk_block_intervals(info)`.
    #    2c. Apply the search parameters to obtain a `list` of `relevant` registers and apris.
    # 3. Print out descriptions of registers, apris, and blocks matching search criteria.

    ####################
    #     PHASE 1      #

    if saves_directory is None:
        saves_directory = Path.cwd()

    elif not isinstance(saves_directory, (Path, str)):
        raise TypeError("`savesDir` must be either a string or of type `pathlib.Path`.")

    saves_directory = resolve_path(Path(saves_directory))

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

        try:
            check_reg_structure(local_dir)

        except FileNotFoundError:
            is_register = False

        else:
            is_register = True

        if is_register:

            ####################
            #     PHASE 2a     #

            # load register
            try:
                reg = Register._from_local_dir(local_dir)

            except (RegisterError, TypeError) as m:
                warnings_.append(f"`Register` at `{str(local_dir)}` not loaded. Error text: {str(m)}")
                continue

            # test if compatible register
            if not reg._has_compatible_version():
                if _args["print_incompatible_regs"]:
                    warnings_.append(f"`Register` at `{str(local_dir)}` has an incompatible version.")
                else:
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
                # if the passed `info` matches ANY of `apris`
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
                # if no search criteria given, then append all info
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

def _wait_for_latency(ident, reg, timeout):

    file = Path.home() / "parallelize.txt"
    digest_file_wait_int = 0.5
    digest_wait_int = 5
    digest_filepath = ident / DIGEST_FILEPATH
    start = time.time()

    while time.time() - start < timeout:

        if digest_filepath.exists():
            break

        else:
            time.sleep(digest_file_wait_int)

    else:

        for d in (Path(ident) / 'register').iterdir():
            print(d)

        raise TimeoutError

    while time.time() - start < timeout:

        digest = read_txt_file(digest_filepath)
        digest_ = reg._digest()
        with file.open("a") as fh:
            fh.write(f"{digest}, {digest_}\n")

        if digest_ == digest:
            return

        else:
            time.sleep(digest_wait_int)

    raise TimeoutError