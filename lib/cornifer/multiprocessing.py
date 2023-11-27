import asyncio
import inspect
import multiprocessing
import time
import warnings
from contextlib import ExitStack

from ._utilities.multiprocessing import make_sigterm_raise_KeyboardInterrupt, process_wrapper
from ._utilities import check_return_int, check_type, check_return_Path_None_default, check_return_int_None_default, \
    resolve_path, num_params, has_variable_num_args
from .debug import log
from .registers import Register
from .errors import RegisterOpenError

async def _timeout_loop(procs, start, timeout):

    while True:

        if all(not proc.is_alive() for proc in procs):
            return

        elif time.time() - start >= timeout:

            for p in procs:

                if p.is_alive():
                    p.terminate()

            return

        else:
            await asyncio.sleep(1)

async def _update_perm_db_loop(procs, update_period, tmp_dir, regs, update_timeout):

    last_update_end = time.time()

    while True:

        if all(not proc.is_alive() for proc in procs):
            return

        elif update_period is not None and tmp_dir is not None and time.time() - last_update_end >= update_period:

            await asyncio.gather(reg._update_perm_db(update_timeout) for reg in regs)
            last_update_end = time.time()

        await asyncio.sleep(1)

async def _pool_loop(procs, conns, param_reg):

    with param_reg._txn('reader') as ro_txn:

        apri_it = param_reg._apris_disk(ro_txn)
        apri = index = msg = None
        intervals_it = iter(())
        indices_it = iter(())

        while True:

            for proc, conn in zip(procs, conns):

                if proc.is_alive():

                    if conn.poll():

                        conn.recv()

                        while True:

                            try:
                                index = next(indices_it)

                            except StopIteration:

                                try:
                                    startn, length = next(intervals_it)

                                except StopIteration:

                                    try:
                                        apri, apri_json = next(apri_it)

                                    except StopIteration:

                                        msg = None
                                        break

                                    else:

                                        prefix = param_reg._intervals_pre(apri, apri_json, False, ro_txn)
                                        intervals_it = param_reg._intervals_disk(prefix, ro_txn)

                                else:
                                    indices_it = range(startn, startn + length)

                            else:

                                msg = (apri, index)
                                break

                        conn.send(msg)

            await asyncio.sleep(0.1)

def _wrap_parallelize_target(target, num_procs, proc_index, args, num_alive_procs, hard_reset_conditions):

    with make_sigterm_raise_KeyboardInterrupt():

        with process_wrapper(num_alive_procs, [], hard_reset_conditions):
            target(num_procs, proc_index, *args)

def _wrap_pool_target(target, param_reg, conn, args, num_alive_procs, hard_reset_conditions):

    with ExitStack() as stack:

        stack.enter_context(make_sigterm_raise_KeyboardInterrupt())
        stack.enter_context(process_wrapper(num_alive_procs, [], hard_reset_conditions))
        stack.enter_context(param_reg.open(readonly = True))

        while True:

            msg = conn.recv()

            if msg is not None:

                apri, index = msg
                target(param_reg, apri, index, *args)

            else:
                return


def _type_value_checks(
    num_procs, target, args, timeout, tmp_dir, update_period, update_timeout, sec_per_block_upper_bound
):

    num_procs = check_return_int(num_procs, "num_procs")

    if not callable(target):
        return TypeError("`target` must be a function.")

    check_type(args, "args", tuple)
    timeout = check_return_int(timeout, "timeout")
    tmp_dir = check_return_Path_None_default(tmp_dir, "tmp_dir", None)
    update_period = check_return_int_None_default(update_period, "update_period", None)
    update_timeout = check_return_int(update_timeout, "update_timeout")
    sec_per_block_upper_bound = check_return_int(sec_per_block_upper_bound, 'sec_per_block_upper_bound')

    if num_procs <= 0:
        raise ValueError("`num_procs` must be positive.")

    if timeout <= 0:
        raise ValueError("`timeout` must be positive.")

    if tmp_dir is not None:
        tmp_dir = resolve_path(tmp_dir)

    regs = tuple(arg for arg in args if isinstance(arg, Register))
    log(f'{regs}')

    for reg in regs:

        if reg._opened:
            raise RegisterOpenError(f"Register `{reg.shorthand()}` cannot be open.")

    if tmp_dir is None and update_period is None:
        warnings.warn(
            'You passed `update_period` to `parallelize, but did not pass `tmp_dir`.'
        )

    if update_period is not None and update_period <= 0:
        raise ValueError("`update_period` must be positive.")

    if update_timeout <= 0:
        raise ValueError("`update_timeout` must be positive.")

    return num_procs, timeout, tmp_dir, update_period, update_timeout, sec_per_block_upper_bound, regs

def pool(
    num_procs, target, param_reg, args = (), timeout = 600, tmp_dir = None, update_period = None, update_timeout = 60,
    sec_per_block_upper_bound = 60
):

    start = time.time()
    check_type(param_reg, 'param_reg', Register)
    num_procs, timeout, tmp_dir, update_period, update_timeout, sec_per_block_upper_bound, regs = _type_value_checks(
        num_procs, target, args, timeout, tmp_dir, update_period, update_timeout, sec_per_block_upper_bound
    )


    if any(arg == param_reg for arg in args):
        raise ValueError("You cannot include `param_reg` among `args`.")

    if param_reg._opened:
        raise RegisterOpenError(f"Register `{param_reg.shorthand()}` cannot be open.")

    num_params_ = num_params(target)

    if num_params_ < 2:
        raise ValueError(
            '`target` function must have at least three parameters. The first three must be, respectively: a '
            '`Register`, an `ApriInfo`, and a non-negative `int`.'
        )

    if not has_variable_num_args(target) and 3 + len(args) > num_params_:
        raise ValueError(
            f'`target` function takes at most {num_params_} parameters, but `args` parameter has length {len(args)} '
            f'(plus 3 for the required parameters).'
        )

    mp_ctx = multiprocessing.get_context("spawn")
    num_alive_procs = mp_ctx.Value('i', 0)
    procs = []

    for reg in regs:

        if tmp_dir is not None:

            log(f'creating update data {reg.shorthand()}')
            reg._create_update_perm_db_shared_data(mp_ctx, update_timeout)

        log(f'creating hard reset data {reg.shorthand()}')
        reg._create_hard_reset_shared_data(mp_ctx, num_alive_procs, 2 * sec_per_block_upper_bound)

    with ExitStack() as stack:

        if tmp_dir is not None:

            for reg in regs:

                log(f'entering tmp_db {reg.shorthand()}')
                stack.enter_context(reg.tmp_db(tmp_dir, update_timeout))

        these_conns = []
        those_conns = []

        for _ in range(num_procs):

            this_conn, that_conn = mp_ctx.Pipe()
            these_conns.append(this_conn)
            those_conns.append(that_conn)

        for that_conn in those_conns:
            procs.append(mp_ctx.Process(
                target = _wrap_pool_target,
                args = (target, param_reg, that_conn, args, num_alive_procs, [reg._hard_reset_condition for reg in regs])
            ))

        async def main():
            await asyncio.gather(
                _update_perm_db_loop(procs, update_period, tmp_dir, regs, update_timeout),
                _timeout_loop(procs, start, timeout),
                _pool_loop(procs, these_conns, param_reg)
            )

        asyncio.run(main())

        for proc in procs:
            proc.join()

def parallelize(
    num_procs, target, args = (), timeout = 600, tmp_dir = None, update_period = None, update_timeout = 60,
    sec_per_block_upper_bound = 60
):

    start = time.time()
    num_procs, timeout, tmp_dir, update_period, update_timeout, sec_per_block_upper_bound, regs = _type_value_checks(
        num_procs, target, args, timeout, tmp_dir, update_period, update_timeout, sec_per_block_upper_bound
    )
    num_params_ = num_params(target)

    if num_params_ < 2:
        raise ValueError(
            "`target` function must have at least two parameters. The first must be `num_procs` (the number of "
            "processes, a positive int) and the second must be `proc_index` (the process index, and int between 0 and "
            "`num_procs-1`, inclusive)."
        )

    if not has_variable_num_args(target) and 2 + len(args) > num_params_:
        raise ValueError(
            f"`target` function takes at most {num_params_} parameters, but `args` parameter has length {len(args)} "
            f"(plus 2 for `num_procs` and `proc_index`)."
        )

    mp_ctx = multiprocessing.get_context("spawn")
    num_alive_procs = mp_ctx.Value('i', 0)
    procs = []

    for reg in regs:

        if tmp_dir is not None:

            log(f'creating update data {reg.shorthand()}')
            reg._create_update_perm_db_shared_data(mp_ctx, update_timeout)

        log(f'creating hard reset data {reg.shorthand()}')
        reg._create_hard_reset_shared_data(mp_ctx, num_alive_procs, 2 * sec_per_block_upper_bound)

    with ExitStack() as stack:

        if tmp_dir is not None:

            for reg in regs:

                log(f'entering tmp_db {reg.shorthand()}')
                stack.enter_context(reg.tmp_db(tmp_dir, update_timeout))

        for proc_index in range(num_procs):
            procs.append(mp_ctx.Process(
                target = _wrap_parallelize_target,
                args = (target, num_procs, proc_index, args, num_alive_procs, [reg._hard_reset_condition for reg in regs])
            ))

        log(f'starting procs')

        for proc in procs:
            proc.start()

        async def main():
            await asyncio.gather(
                _update_perm_db_loop(procs, update_period, tmp_dir, regs, update_timeout),
                _timeout_loop(procs, start, timeout)
            )

        asyncio.run(main())

        for proc in procs:
            proc.join()