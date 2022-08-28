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
import warnings
from pathlib import Path

from cornifer.errors import RegisterError
from cornifer.registers import Register
from cornifer.regfilestructure import LOCAL_DIR_CHARS, checkRegStructure
from cornifer._utilities import resolvePath
from cornifer.version import CURRENT_VERSION, COMPATIBLE_VERSIONS

_ARGS_TYPES = {
    "regLimit" : int,

    "printApri" : bool,
    "apriLimit" : int,

    "printIntervals" : bool,
    "printIntervalMode" : str,
    "intervalLimit" : int,

    "printIncompatibleRegs" : bool,

    "keyExactMatch" : bool,

    "tupleExactMatch" : bool,

    "dictExactMatch" : bool,

    "strExactMatch" : bool,

}

_args = {
    "regLimit" : 10,

    "printApri" : True,
    "apriLimit" : 5,

    "printIntervals" : True,
    "printIntervalMode" : "combined", # combined, uncombined
    "intervalLimit" : 5,

    "print_warnings_" : True,
    "warnings_limit" : 10,

    "printIncompatibleRegs" : False,

    "keyExactMatch" : False,

    "tupleExactMatch" : False,

    "dictExactMatch" : False,

    "strExactMatch" : False
}

def setSearchArgs(**kwargs):
    """This function changes the output of the `search` function."""

    for key,val in kwargs:

        if key not in _ARGS_TYPES.keys():
            raise KeyError(f"Unrecognized `search_arg` key: {key}")

        elif not isinstance(val, _ARGS_TYPES[key]):
            raise TypeError(f"Expected type for key \"{key}\" value : {_ARGS_TYPES[key].__name__}")

        elif _ARGS_TYPES[key] == int and val <= 0:
            raise ValueError(f"Value for key \"{key}\" must be a positive integer.")

        elif key == "printIntervals_mode" and val not in ["combined", "uncombined"]:
            raise ValueError('Value for key "printIntervals_mode" can be either "combined" or "uncombined".')

        _args[key] = val

def load(identifier):

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

    reg = Register._fromLocalDir(resolved)

    if not reg._hasCompatibleVersion():
        warnings.warn(
            f"The register at `{reg._localDir}` has an incompatible version.\n"
            f"Current Cornifer version: {CURRENT_VERSION}\n"
            f"Compatible versions:      {str(COMPATIBLE_VERSIONS)}\n"
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
    #    `reg.get_all_intervals(apri, combine = True)` and those of `uncombined` are the return values of
    #    `reg.get_disk_block_intervals(apri)`.
    #    2c. Apply the search parameters to obtain a `list` of `relevant` registers and apris.
    # 3. Print out descriptions of registers, apris, and blocks matching search criteria.

    ####################
    #     PHASE 1      #

    if saves_directory is None:
        saves_directory = Path.cwd()

    elif not isinstance(saves_directory, (Path, str)):
        raise TypeError("`savesDir` must be either a string or of type `pathlib.Path`.")

    saves_directory = resolvePath(Path(saves_directory))

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
            checkRegStructure(local_dir)

        except FileNotFoundError:
            is_register = False

        else:
            is_register = True

        if is_register:

            ####################
            #     PHASE 2a     #

            # load register
            try:
                reg = Register._fromLocalDir(local_dir)

            except (RegisterError, TypeError) as m:
                warnings_.append(f"`Register` at `{str(local_dir)}` not loaded. Error text: {str(m)}")
                continue

            # test if compatible register
            if not reg._hasCompatibleVersion():
                if _args["printIncompatibleRegs"]:
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


                # mode = _args["printIntervalMode"]

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
                                _key not in _apri._reservedKws and (
                                    (key == _key and _args["keyExactMatch"]) or
                                    (key_re.match(_key) is not None and not _args["keyExactMatch"])
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

    relevant = sorted(relevant, key = lambda t: t[0]._localDir)
    current_reg = None
    reg_index = 0
    apri_index = 0
    hit_apriLimit = False

    prnt += "REGISTERS:\n"
    for reg,apri in relevant:

        if current_reg is None or current_reg != reg:

            current_reg = reg
            prnt += f"({reg._localDir.name}) \"{str(reg)}\"\n"
            hit_apriLimit = False
            apri_index = 0

            if current_reg is not None:
                reg_index += 1

            else:
                reg_index = 0

        if reg_index >= _args["regLimit"]:

            num_regs = len(set(_reg for _reg, _ in relevant))
            prnt += f"... and {num_regs - reg_index} more.\n"
            break

        if _args["printApri"] and not hit_apriLimit:

            if apri_index >= _args["apriLimit"]:

                hit_apriLimit = True
                num_apri = len(set(_apri for _reg,_apri in relevant if _reg == reg))
                prnt += f"... and {num_apri - apri_index} more.\n"

            else:

                prnt += f"\t{repr(apri)}\n"

                if _args["printIntervals"]:

                    lim = _args["intervalLimit"]

                    if _args["printIntervalMode"] == "combined":
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
        if _args["strExactMatch"]:
            search_val = re.compile(search_val)
        else:
            return search_val == apri_val

    if isinstance(search_val, re.Pattern):
        return search_val.match(apri_val) is not None

    elif isinstance(search_val, dict):
        if _args["dictExactMatch"]:
            return search_val == apri_val
        else:
            return all(val == apri_val[key] for key, val in search_val.items())

    elif isinstance(search_val, tuple):
        if _args["tupleExactMatch"]:
            return search_val == apri_val
        else:
            return search_val in apri_val
    else:
        return search_val == apri_val
        