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

from contextlib import contextmanager, ExitStack, AbstractContextManager
import argparse
from pathlib import Path

from ._utilities import check_type, check_return_int, check_type_None_default, check_return_Path_None_default, \
    check_return_int_None_default, resolve_path, is_deletable
from ._utilities.multiprocessing import start_with_timeout, process_wrapper, make_sigterm_raise_ReceivedSigterm
from .info import ApriInfo, AposInfo
from .blocks import Block
from .multiprocessing import parallelize
from .registers import Register, PickleRegister, NumpyRegister
from .regloader import search, load_ident, load
from .errors import DataNotFoundError, CompressionError, DecompressionError, RegisterError, RegisterOpenError

__all__ = [
    "ApriInfo",
    "AposInfo",
    "Block",
    "Register",
    "PickleRegister",
    "NumpyRegister",
    "search",
    "load_ident",
    "load",
    "DataNotFoundError",
    "CompressionError",
    "DecompressionError",
    "RegisterError",
    "stack",
    "parallelize"
]

@contextmanager
def stack(*cms):

    yield_ = []

    with ExitStack() as stack_:

        for cm in cms:
            yield_.append(stack_.enter_context(cm))

        yield tuple(yield_)