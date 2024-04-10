import argparse
import collections
import copy
import datetime
import functools
import re
import shutil
import statistics
import subprocess
from pathlib import Path

from .errors import CannotLoadError, DataNotFoundError
from .debug import _file_datetime_format, _line_datetime_format, _line_datetime_len
from .regloader import _load, _load_ident

def resolved_Path(str_):

    path = Path(str_)

    if path.is_absolute():
        return path

    else:
        return Path.cwd() / str_

def _add_shorthand_ident_arguments(parser):

    parser.add_argument(
        '-i', '--ident', help = 'Flags a register identifier.', metavar = 'ID', dest = 'idents', nargs = '+',
        default = []
    )
    parser.add_argument(
        'shorthands', help = 'List of register shorthands.', metavar = 'SH', nargs = '*', default = []
    )

def _add_save_dir_argument(parser):
    parser.add_argument(
        '-d', '--dir', help = 'Register(s) parent directory (default: current directory)', default = Path.cwd(),
        type = resolved_Path
    )

def _add_target_dir_argument(parser):
    parser.add_argument('-t', '--target', help = 'Target directory.', nargs = 1, required = True, type = resolved_Path)

def _add_verbose_argument(parser):
    parser.add_argument('-v', '--verbose', help = 'Display additional info.', action = 'store_true')

def _add_force_argument(parser):
    parser.add_argument('-f', '--force', help = 'Ignore errors.', action = 'store_true')

def _separate(line):
    return (
        datetime.datetime.strptime(line[:_line_datetime_len], _line_datetime_format),
        line[_line_datetime_len + 1:].strip()
    )

def _load_regs(shorthands, idents, dir_):

    do_all = len(shorthands) == 0 and len(idents) == 0
    regs = []
    missing = ''

    if do_all:

        for d in dir_.iterdir():

            try:
                reg = _load_ident(d.name, dir_)

            except CannotLoadError:
                pass

            else:
                regs.append(reg)

    else:

        for shorthand in shorthands:

            try:
                regs_ = _load(shorthand, dir_)

            except CannotLoadError:
                pass

            else:
                regs.extend(regs_)

        for ident in idents:

            try:
                reg = _load_ident(ident, dir_)

            except CannotLoadError:
                pass

            else:
                regs.append(reg)

        remaining_shorthands = copy.copy(args.shorthands)
        remaining_idents = copy.copy(args.idents)

        for reg in regs:

            try:
                remaining_shorthands.remove(reg.shorthand())

            except ValueError:
                pass

            try:
                remaining_idents.remove(reg.ident())

            except ValueError:
                pass

        if len(remaining_shorthands) > 0:
            missing += f'Could not find the following shorthands: {", ".join(remaining_shorthands)}\n'

        if len(remaining_idents) > 0:
            missing += f'Could not find the following idents: {", ".join(remaining_idents)}\n'

    if len(regs) == 0:
        raise DataNotFoundError(f"No matching Registers found in {dir_}")

    return list(collections.OrderedDict([(reg, None) for reg in regs]).keys()), missing

def _check_raise_directory(path):

    if not path.exists():
        raise FileNotFoundError(path)

    elif not path.is_dir():
        raise NotADirectoryError(path)

def _check_plain_file(path):

    if not path.exists():
        raise FileNotFoundError(path)

    elif path.is_dir():
        raise IsADirectoryError(path)


###########################
#         COMMAND         #
parser_command = argparse.ArgumentParser(
    prog = 'Cornifer',
    description = 'Command line utility for Cornifer.'
)
subparsers = parser_command.add_subparsers(required = True, dest ='command')
###########################
#         SUMMARY         #
parser_summary = subparsers.add_parser('summary', help = 'Print register summaries.')
_add_save_dir_argument(parser_summary)
_add_shorthand_ident_arguments(parser_summary)
###########################
#          DEBUG          #
NLINES_DEFAULT = -10
FIRST_DEFAULT = -10
LAST_DEFAULT = -1
STEP_DEFAULT = 1
parser_debug = subparsers.add_parser('debug', help = 'Display debug file info.')
mutex_group = parser_debug.add_mutually_exclusive_group()
mutex_group.add_argument(
    '-n', '--nlines', help = f'Number of lines to display (default: {NLINES_DEFAULT}) (positive: forward from first '
    'line, negative: backward from last line, zero: all lines)', default = NLINES_DEFAULT, type = int
)
absolute_lines_group = mutex_group.add_argument_group(
    'Absolute lines. Positive numbers count forward from the first line, negative backwards from the last.'
)
absolute_lines_group.add_argument(
    '-f', '--first', help = f'First line to display (default: {FIRST_DEFAULT})', default = FIRST_DEFAULT, type = int
)
absolute_lines_group.add_argument(
    '-l', '--last', help = f'Last line to display (default: {LAST_DEFAULT})', default = LAST_DEFAULT, type = int
)
absolute_lines_group.add_argument(
    '-s', '--step', help = f'Step (default: {STEP_DEFAULT})', default = STEP_DEFAULT, type = int
)
parser_debug.add_argument(
    '-d', '--dir', help = 'Directory of debug file (default: cwd)', default = Path.cwd(), type = resolved_Path
)
parser_debug.add_argument(
    '-t', '--time', help = 'Display lines from time file (default: most recent file)',
    type = lambda arg: datetime.datetime.strptime(arg, _file_datetime_format)
)
parser_debug.add_argument(
    '-p', '--pids', help = 'Process IDs to display (default: all pids)', default = None, nargs = '+'
)
parser_debug.add_argument(
    '-r', '--regex', help = 'Filter lines matching given regex (default: no filter)', type = re.compile
)
elapsed_group = parser_debug.add_argument_group(
    'Calculate elapsed time statistics.', 'Timer starts at any line matching option -e1 and ends at first following '
    'line that matches option -e2.')
elapsed_group.add_argument('-e1', '--elapsed1', help = 'Regex', type = re.compile)
elapsed_group.add_argument('-e2', '--elapsed2', help = 'Regex', type = re.compile)
elapsed_group.add_argument('-S', '--sum', help = 'Display sum of all elapsed times', action = 'store_true')
elapsed_group.add_argument('-m', '--mean', help = 'Display mean elapsed time', action = 'store_true')
elapsed_group.add_argument('-M', '--median', help = 'Display median elapsed time', action = 'store_true')
elapsed_group.add_argument('-c', '--count', help = 'Display number elapsed times', action = 'store_true')
elapsed_group.add_argument('--min', help = 'Display min time', action = 'store_true')
elapsed_group.add_argument('--max', help = 'Display max time', action = 'store_true')
elapsed_group.add_argument('--spread', help = 'Display spread', action = 'store_true')
elapsed_group.add_argument('-q', '--quants', help = 'Number of quantiles to display (default: none)', type = int)

###########################
#          DELETE         #
parser_delete = subparsers.add_parser('delete', help = 'Delete registers.')
_add_save_dir_argument(parser_delete)
_add_shorthand_ident_arguments(parser_delete)
_add_verbose_argument(parser_delete)
###########################
#         SLURMIFY        #
parser_slurmify = subparsers.add_parser(
    'slurmify', help = 'Submit a Python script to Slurm for execution. Any options not listed below will be forwarded '
    'to `sbatch`.'
)
parser_slurmify.add_argument('main', help = 'Python script to run with command-line args', type = resolved_Path, nargs = '+')
parser_slurmify.add_argument('--ncpu', help = 'Number of CPUs (default: 1)', default = 1, type = int)
parser_slurmify.add_argument('--email', help = 'Email this address when script starts/finishes (default: no emails)')
parser_slurmify.add_argument('--job-name', help = 'Slurm job name (default: CorniferScript)', dest = 'job_name')
_add_verbose_argument(parser_slurmify)

#
# parser_move = subparsers.add_parser('move', help = 'Move registers to another directory.')
# add_save_dir_argument(parser_move)
# add_shorthand_ident_arguments(parser_move)
# add_target_dir_argument(parser_move)
#
# parser_copy = subparsers.add_parser('copy', help = 'Copy registers to another directory.')
# add_save_dir_argument(parser_copy)
# add_shorthand_ident_arguments(parser_copy)
# add_target_dir_argument(parser_copy)

args, unrecognized = parser_command.parse_known_args()


# parser_search = subparsers.add_parser('search', help = 'hi')
# parser_merge = subparsers.add_parser('merge', help = 'hi')
# parser_help = subparsers.add_parser('help', help = 'hi')


if args.command == 'summary':

    parser_command.parse_args()  # no unknown args
    regs, to_print = _load_regs(args.shorthands, args.idents, args.dir)

    if len(to_print) > 0:
        to_print += '\n'

    for reg in regs:

        with reg.open(True):
            to_print += reg.summary() + '\n\n'

    print(to_print)

elif args.command == 'debug':

    parser_command.parse_args()  # no unknown args
    display_nlines = args.first is None and args.last is None and args.step is None
    _check_raise_directory(args.dir)

    if args.time is not None:

        args.time = (args.time, args.dir / args.time.strftime(_file_datetime_format))
        _check_raise_directory(args.time[1])

    else:

        dts = []

        for time_dir in args.dir.iterdir():

            if time_dir.is_dir():

                try:
                    dt = datetime.datetime.strptime(time_dir.name, _file_datetime_format)

                except ValueError:
                    pass

                else:
                    dts.append(dt)

        if len(dts) == 0:
            raise FileNotFoundError('No debug files found')

        dt = max(dts)
        args.time = args.dir / dt.strftime(_file_datetime_format)

    if args.pids is not None:

        for i, pid in enumerate(args.pids):

            pid_file = args.time / f'{pid}.txt'
            _check_plain_file(pid_file)
            args.pids[i] = (pid, pid_file)

    else:

        args.pids = []

        for pid_file in args.time.iterdir():

            if pid_file.is_file() and re.search(r'\d+\.txt', pid_file.name) is not None:
                args.pids.append((pid_file.stem, pid_file))

        if len(args.pids) == 0:
            raise FileNotFoundError('No debug files found')

    print(f'Displaying debug info from {args.time}')

    if display_nlines:

        if args.nlines > 0:

            for pid, pid_file in args.pids:

                print(pid)

                with pid_file.open('r') as fh:

                    for i, line in enumerate(fh.readlines()):

                        if i >= args.nlines:
                            break



    # if d1 is None:
    #
    #     for pid in pids:
    #
    #         print(pid)
    #         pid_file = dir_ / f'{pid}.txt'
    #         to_print = []
    #
    #         with pid_file.open('r') as fh:
    #
    #             for i, line in enumerate(fh.readlines()):
    #
    #                 i += 1
    #
    #                 if memory > 0 and len(to_print) < memory:
    #                     to_print.append(f'\t{i:08d}, {line.strip()}')
    #
    #                 elif memory > 0:
    #                     break
    #
    #                 elif memory < 0:
    #
    #                     if len(to_print) == -memory:
    #                         del to_print[0]
    #
    #                     to_print.append(f'\t{i:08d}, {line.strip()}')
    #
    #         for line in to_print:
    #             print(line)
    #
    # else:
    #
    #     for pid in pids:
    #
    #         print(pid)
    #         pid_file = dir_ / f'{pid}.txt'
    #         to_print = []
    #         stats = []
    #         start_time = None
    #
    #         with pid_file.open('r') as fh:
    #
    #             for i, line in enumerate(fh.readlines()):
    #
    #                 dt, msg = _separate(line)
    #
    #                 if msg == d1 and start_time is None:
    #                     start_time = dt
    #
    #                 elif msg == d2 and start_time is not None:
    #
    #                     stats.append((dir_time - start_time).total_seconds())
    #                     start_time = None
    #
    #         if len(stats) > 0:
    #             print(
    #                 pid, statistics.mean(stats), len(stats), sum(stats), statistics.stdev(stats), min(stats),
    #                 [str(t) for t in statistics.quantiles(stats, n = 8)], max(stats)
    #             )

elif args.command == 'delete':

    parser_command.parse_args() # no unknown args
    regs, to_print = _load_regs(args.shorthands, args.idents, args.dir)

    for reg in regs:
        to_print += str(reg)

    if len(to_print) > 0:
        print(to_print)

    confirm = ''

    while confirm.strip().lower() not in ('y', 'n'):

        confirm = input('Permanently delete the registers above? (y/N) ')

        if confirm.strip() == '':
            confirm = 'n'

    if confirm == 'y':

        for reg in regs:

            if args.verbose:
                print(f'Deleting {reg}')

            try:
                shutil.rmtree(reg._local_dir)

            except BaseException as e:

                if not args.force:
                    raise

                elif args.verbose:
                    print(f'Failed to delete {reg}, error: {repr(e)}')

    else:
        print('No confirmation given. Nothing deleted.')

elif args.command == 'slurmify':

    if '--nodes' in unrecognized or '-N' in unrecognized:
        raise ValueError(
            '`slurmify` does not accept either of the options -N or --nodes (the hardcoded value is 1). You must use '
            '`sbatch` manually if you wish to change the number of nodes.'
        )

    if '--ntasks' in unrecognized or '-n' in unrecognized:
        raise ValueError(
            '`slurmify` does not accept either of the options -n or --ntasks (the hardcoded value is 1). You must use '
            '`sbatch` manually if you wish to change the number of tasks.'
        )

    sbatch_args = [
        '--job-name', args.job_name,
        '--nodes', '1',
        '--ntasks', '1',
        '--cpus-per-task', str(args.ncpu)
    ]

    if args.email is not None:
        sbatch_args += [
            '--mail-user', args.email,
            '--mail-type', 'ALL'
        ]

    sbatch_args.extend(unrecognized)
    sbatch_args = ['sbatch'] + sbatch_args + [args.main]
    sbatch_command = " ".join(sbatch_args)

    if args.verbose:
        print(f'Running `{sbatch_command}`')

    sbatch_capture = subprocess.run(sbatch_args, text = True, capture_output = True)

    if sbatch_capture.stderr != '':
        raise RuntimeError(f'`{sbatch_command}` errored out: {sbatch_capture.stderr}')

    print(sbatch_capture.stdout)

else:
    raise NotImplementedError
