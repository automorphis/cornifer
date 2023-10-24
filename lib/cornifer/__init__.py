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
import inspect
import multiprocessing
import time
import warnings

from contextlib import contextmanager, ExitStack, AbstractContextManager
from datetime import datetime
from pathlib import Path

from ._utilities import check_type, check_return_int, check_type_None_default, check_Path_None_default, \
    check_return_int_None_default, resolve_path, is_deletable
from ._utilities.multiprocessing import start_with_timeout
from .info import ApriInfo, AposInfo
from .blocks import Block
from .registers import Register, PickleRegister, NumpyRegister
from .regloader import search, load_ident, load_shorthand
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
    "load_shorthand",
    "DataNotFoundError",
    "CompressionError",
    "DecompressionError",
    "RegisterError",
    "openregs",
    "openblks",
    "parallelize"
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

    with ExitStack() as stack:
        yield tuple([stack.enter_context(blk) for blk in blks])

def _wrap_target(target, num_procs, proc_index, args, regs, num_active_txns, txn_wait_event):

    for reg in regs:
        reg._set_txn_shared_data(num_active_txns, txn_wait_event)

    target(num_procs, proc_index, *args)

def parallelize(num_procs, target, args = (), timeout = 600, tmp_dir = None, regs = (), update_period = None, update_timeout = 60):

    start = time.time()
    num_procs = check_return_int(num_procs, "num_procs")

    if not callable(target):
        return TypeError("`target` must be a function.")

    check_type(args, "args", tuple)
    timeout = check_return_int(timeout, "timeout")
    check_Path_None_default(tmp_dir, "tmp_dir", None)
    check_type(regs, "regs", tuple)
    update_period = check_return_int_None_default(update_period, "update_period", None)
    update_timeout = check_return_int(update_timeout, "update_timeout")

    if num_procs <= 0:
        raise ValueError("`num_procs` must be positive.")

    num_params = len(inspect.signature(target).parameters)

    if num_params < 2:
        raise ValueError(
            "`target` function must have at least two parameters. The first must be `num_procs` (the number of "
            "processes, a positive int) and the second must be `proc_index` (the process index, and int between 0 and "
            "`num_procs-1`, inclusive)."
        )

    has_variable_num_args = any(
        param.kind in (inspect.Parameter.VAR_POSITIONAL, inspect.Parameter.VAR_KEYWORD)
        for param in inspect.signature(target).parameters.values()
    )

    if not has_variable_num_args and 2 + len(args) > num_params:
        raise ValueError(
            f"`target` function takes at most {num_params} parameters, but `args` parameter has length {len(args)} "
            f"(plus 2 for `num_procs` and `proc_index`)."
        )

    if timeout <= 0:
        raise ValueError("`timeout` must be positive.")

    if tmp_dir is not None:
        tmp_dir = resolve_path(tmp_dir)

    for i, reg in enumerate(regs):

        check_type(reg, f"regs[{i}]", Register)

        if reg._opened:
            raise RegisterOpenError(f"`regs[{i}]` cannot be open during a call to `parallelize`.")

    if tmp_dir is not None and len(regs) == 0:
        warnings.warn(
            f"You passed `tmp_dir` to `parallelize`, but did not pass `regs`."
        )

    elif tmp_dir is None and len(regs) > 0:
        warnings.warn(
            f"You passed `regs` to `parallelize`, but did not pass `tmp_dir`."
        )

    if update_period is not None and update_period <= 0:
        raise ValueError("`update_period` must be positive.")

    if update_timeout <= 0:
        raise ValueError("`update_timeout` must be positive.")

    file = Path.home() / "parallelize.txt"
    mp_ctx = multiprocessing.get_context("spawn")
    procs = []
    update = update_period is not None and tmp_dir is not None
    txn_wait_event = mp_ctx.Event()
    txn_wait_event.set() # transactions initially do not have to wait
    num_active_txns = mp_ctx.Value("i", 0)
    timeout_wait_period = 0.5
    update_wait_period = 0.1
    with file.open("w") as fh:
        fh.write("1\n")

    with ExitStack() as stack:

        if tmp_dir is not None:

            for reg in regs:
                with file.open("a") as fh:
                    for d in reg._perm_db_filepath.iterdir():
                        fh.write(f"{d} {is_deletable(d)}\n")
                stack.enter_context(reg.tmp_db(tmp_dir, update_period))
                with file.open("a") as fh:
                    for d in reg._perm_db_filepath.iterdir():
                        fh.write(f"{d} {is_deletable(d)}\n")

        with file.open("a") as fh:
            fh.write("2\n")

        for proc_index in range(num_procs):
            procs.append(mp_ctx.Process(
                target = _wrap_target,
                args = (target, num_procs, proc_index, args, regs, num_active_txns, txn_wait_event)
            ))

        with file.open("a") as fh:
            for d in regs[0]._perm_db_filepath.iterdir():
                fh.write(f"{d} {is_deletable(d)}\n")

        with file.open("a") as fh:
            fh.write("3\n")

        for proc in procs:
            with file.open("a") as fh:
                fh.write("4\n")
            with file.open("a") as fh:
                for d in regs[0]._perm_db_filepath.iterdir():
                    fh.write(f"{d} {is_deletable(d)}\n")
            proc.start()
            with file.open("a") as fh:
                fh.write("5\n")
            with file.open("a") as fh:
                for d in regs[0]._perm_db_filepath.iterdir():
                    fh.write(f"{d} {is_deletable(d)}\n")

        last_update_end = time.time()

        with file.open("a") as fh:
            fh.write("6\n")

        while True: # timeout loop

            with reg.open(readonly = True):

                with file.open("a") as fh:
                    fh.write(f"timeout loop {datetime.now().strftime('%H:%M:%S.%f')} {regs[0].num_apri()}\n")

            if time.time() - start >= timeout:

                for p in procs:
                    p.terminate()

                break # timeout loop

            elif all(not proc.is_alive() for proc in procs):
                break # timeout loop

            elif update and time.time() - last_update_end >= update_period:

                update_start = time.time()
                txn_wait_event.clear() # block future transactions

                while True: # update loop
                    with file.open("a") as fh:
                        fh.write(f"update loop {datetime.now().strftime('%H:%M:%S.%f')} {num_active_txns.value}\n")
                    # wait for current transactions to complete before updating
                    if num_active_txns.value == 0:
                        with file.open("a") as fh:
                            fh.write("7\n")
                        for reg in regs:

                            with file.open("a") as fh:
                                fh.write("8\n")
                            with file.open("a") as fh:
                                for d in regs[0]._perm_db_filepath.iterdir():
                                    fh.write(f"{d} {is_deletable(d)}\n")
                            with file.open("a") as fh:
                                fh.write("9\n")
                            reg.update_perm_db(update_timeout + update_start - time.time())
                            with file.open("a") as fh:
                                fh.write("10\n")
                            with file.open("a") as fh:
                                for d in regs[0]._perm_db_filepath.iterdir():
                                    fh.write(f"{d} {is_deletable(d)}\n")

                        txn_wait_event.set() # allow transactions
                        break # update loop

                    else:
                        time.sleep(update_wait_period)

                    if time.time() - update_start >= update_timeout:

                        warnings.warn("Permanent `Register` periodic update timed out.")
                        break # update loop

                last_update_end = time.time()

            time.sleep(timeout_wait_period)

        for proc in procs:
            proc.join()