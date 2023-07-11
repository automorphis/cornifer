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

from .info import ApriInfo, AposInfo
from .blocks import Block
from .registers import Register, PickleRegister, NumpyRegister
from .regloader import search, load
from .errors import DataNotFoundError, CompressionError, DecompressionError, RegisterError

__all__ = [
    "ApriInfo",
    "AposInfo",
    "Block",
    "Register",
    "PickleRegister",
    "NumpyRegister",
    "search",
    "load",
    "DataNotFoundError",
    "CompressionError",
    "DecompressionError",
    "RegisterError",
    "openregs",
    "openblks"
]

@contextmanager
def openregs(*regs, **kwargs):
    """Syntactic sugar. Opens many `Register`s at once for reading and/or writing. Similar to `openblks`.

    The snippet:

        with reg1.open(readonly = True) as reg1:
            with reg2.open() as reg2:
                ...

    is equivalent to

        with Register.opens(reg1, reg2, readonlys = (True, False)) as (reg1, reg2):
            ...

    Note that the parentheses MUST be present after the `as`, otherwise Python will get confused.

    :param regs:
    :param kwargs:
    :return:
    """

    if (len(kwargs) == 1 and 'readonlys' not in kwargs) or len(kwargs) > 1:
        raise KeyError("`opens` only takes one keyword-argument, `readonlys`.")

    if len(kwargs) == 1:
        readonlys = kwargs['readonlys']

    else:
        readonlys = None

    if readonlys is not None and not isinstance(readonlys, (list, tuple)):
        raise TypeError("`readonlys` must be of type `list` or `tuple`.")

    for reg in regs:

        if not isinstance(reg, Register):
            raise TypeError("Each element of `regs` must be of type `Register`.")

    if readonlys is not None:

        for readonly in readonlys:

            if not isinstance(readonly, bool):
                raise TypeError("Each element of `readonlys` must be of type `bool`.")

    if readonlys is not None and len(regs) != len(readonlys):
        raise ValueError("`regs` and `readonlys` must have the same length.")

    if readonlys is None:
        readonlys = (False,) * len(regs)

    stack = ExitStack()
    yld = []

    with stack:

        for reg, readonly in zip(regs, readonlys):
            yld.append(stack.enter_context(reg.open(readonly=readonly)))

        yield tuple(yld)

@contextmanager
def openblks(*blks):
    """Open several `Block`s at once. Similar to `Register.opens`.

    The snippet

        reg = NumpyRegister(...)
        blk1 = Block(...)
        with blk1 as blk1:
            with reg.blk(...) as blk2:
                ...

    is equivalent to

        reg = NumpyRegister(...)
        blk1 = Block(...)
        with openblks(blk1, reg.blk(...)) as (blk1, blk2):
            ...

    Note that the parentheses MUST be present after the `as`, otherwise Python will get confused.

    :param blks:
    :return:
    """

    for i, blk in enumerate(blks):
        # The following checks that `blk` is either a `Block` or an instance of the generator `Register.blk`.
        # it's pretty hacky, shrug
        if (
            not isinstance(blk, Block) and (
                not isinstance(blk, AbstractContextManager) or
                len(blk.args) == 0 or
                not isinstance(blk.args[0], Register) or
                blk.gen.gi_code.co_name != "blk"
            )
        ):
            raise TypeError(f"parameter {i} must be of type `Block`, not `{type(blk)}`")

    stack = ExitStack()

    with stack:
        yield tuple([stack.enter_context(blk) for blk in blks])
