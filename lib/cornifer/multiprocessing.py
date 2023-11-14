import asyncio
import inspect
import multiprocessing
import time
import warnings
from contextlib import ExitStack

from . import _utilities
from ._utilities.multiprocessing import make_sigterm_raise_KeyboardInterrupt, process_wrapper
from ._utilities import check_return_int, check_type, check_Path_None_default, check_return_int_None_default, \
    resolve_path, print_debug
from .registers import Register
from .errors import RegisterOpenError

def _wrap_target(target, num_procs, proc_index, args, num_alive_procs, hard_reset_conditions, debug_dir):

    from . import _utilities

    make_sigterm_raise_KeyboardInterrupt()
    _utilities.debug_dir = debug_dir

    with process_wrapper(num_alive_procs, [], hard_reset_conditions):
        target(num_procs, proc_index, *args)


def parallelize(
    num_procs, target, args = (), timeout = 600, tmp_dir = None, update_period = None, update_timeout = 60,
    sec_per_block_upper_bound = 60, debug_dir = None
):

    start = time.time()
    num_procs = check_return_int(num_procs, "num_procs")

    if not callable(target):
        return TypeError("`target` must be a function.")

    check_type(args, "args", tuple)
    timeout = check_return_int(timeout, "timeout")
    check_Path_None_default(tmp_dir, "tmp_dir", None)
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

    regs = tuple(arg for arg in args if isinstance(arg, Register))

    print_debug(f'{regs}')

    for reg in regs:

        if reg._opened:
            raise RegisterOpenError(f"Register `{reg.shorthand()}` cannot be open during a call to `parallelize`.")

    if tmp_dir is None and update_period is None:
        warnings.warn(
            'You passed `update_period` to `parallelize, but did not pass `tmp_dir`.'
        )

    if update_period is not None and update_period <= 0:
        raise ValueError("`update_period` must be positive.")

    if update_timeout <= 0:
        raise ValueError("`update_timeout` must be positive.")

    async def update_all_perm_dbs():

        for reg_ in regs:
            await reg_._update_perm_db(update_timeout)

    mp_ctx = multiprocessing.get_context("spawn")
    num_alive_procs = mp_ctx.Value('i', 0)
    procs = []

    for reg in regs:

        if tmp_dir is not None:

            print_debug(f'creating update data {reg.shorthand()}')
            reg._create_update_perm_db_shared_data(mp_ctx, update_timeout)

        print_debug(f'creating hard reset data {reg.shorthand()}')
        reg._create_hard_reset_shared_data(mp_ctx, num_alive_procs, 2 * sec_per_block_upper_bound)

    with ExitStack() as stack:

        if tmp_dir is not None:

            for reg in regs:

                print_debug(f'entering tmp_db {reg.shorthand()}')
                stack.enter_context(reg.tmp_db(tmp_dir, update_timeout))

        for proc_index in range(num_procs):
            procs.append(mp_ctx.Process(
                target = _wrap_target,
                args = (target, num_procs, proc_index, args, num_alive_procs, [reg._hard_reset_condition for reg in regs], debug_dir)
            ))

        print_debug(f'starting procs')

        for proc in procs:
            proc.start()

        last_update_end = time.time()

        while True: # timeout loop

            print_debug(f'timeout loop')

            if time.time() - start >= timeout:

                print_debug(f'terminating procs')

                for p in procs:
                    p.terminate()

                print_debug(f'procs terminated')
                break # timeout loop

            elif all(not proc.is_alive() for proc in procs):
                break # timeout loop

            elif update_period is not None and tmp_dir is not None and time.time() - last_update_end >= update_period:

                print_debug(f'update block in timeout loop')
                asyncio.run(update_all_perm_dbs())
                last_update_end = time.time()

            time.sleep(1)

        for proc in procs:
            proc.join()