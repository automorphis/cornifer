import argparse
import collections
import copy
import datetime
import re
import shutil
import statistics
import sys
from pathlib import Path

from .errors import CannotLoadError
from .debug import _file_datetime_format, _line_datetime_format, _line_datetime_len
from .regloader import _load, _load_ident


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
        type = Path
    )

def _add_target_dir_argument(parser):
    parser.add_argument('-t', '--target', help = 'Target directory.', nargs = 1, required = True, type = Path)

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

    return list(collections.OrderedDict([(reg, None) for reg in regs]).keys())


parser_command = argparse.ArgumentParser(
    prog = 'Cornifer',
    description = 'Command line utility for Cornifer.'
)
subparsers = parser_command.add_subparsers(required = True, dest ='command')

parser_summary = subparsers.add_parser('summary', help = 'Print register summaries.')
_add_save_dir_argument(parser_summary)
_add_shorthand_ident_arguments(parser_summary)

parser_debug = subparsers.add_parser('debug', help = 'Display debug file info.')
parser_debug.add_argument('-m', '--memory', default = -10, type = int)
parser_debug.add_argument('-p', '--pids', nargs = '*')
parser_debug.add_argument('-s', '--start')
parser_debug.add_argument('-e', '--end')
parser_debug.add_argument('-d', '--dir')
parser_debug.add_argument('-t', '--time')

parser_delete = subparsers.add_parser('delete', help = 'Delete registers.')
_add_save_dir_argument(parser_delete)
_add_shorthand_ident_arguments(parser_delete)
_add_verbose_argument(parser_delete)


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

args = parser_command.parse_args()


# parser_search = subparsers.add_parser('search', help = 'hi')
# parser_merge = subparsers.add_parser('merge', help = 'hi')
# parser_help = subparsers.add_parser('help', help = 'hi')

if args.command == 'summary':

    remaining_shorthands = copy.copy(args.shorthands)
    remaining_idents = copy.copy(args.idents)
    do_all = len(args.shorthands) == 0 and len(args.idents) == 0
    regs = _load_regs(args.shorthands, args.idents, args.dir)

    if not do_all:

        for reg in regs:

            try:
                remaining_shorthands.remove(reg.shorthand())

            except ValueError:
                pass

            try:
                remaining_idents.remove(reg.ident())

            except ValueError:
                pass

    to_print = ''

    if len(remaining_shorthands) > 0:
        to_print += f'COULD NOT FIND THE FOLLOWING SHORTHANDS: {", ".join(remaining_shorthands)}\n'

    if len(remaining_idents) > 0:
        to_print += f'COULD NOT FIND THE FOLLOWING IDENTS: {", ".join(remaining_idents)}\n'

    if len(to_print) > 0 and len(regs) > 0:
        to_print += '\n'

    for reg in regs:
        to_print += str(reg) + '\n\n'

    print(to_print)

elif args.command == 'debug':

    if (args.start is None) != (args.end is None):
        raise ValueError

    if args.start is not None:
        d1, d2 = args.start, args.end

    else:
        d1 = d2 = None

    if args.dir is not None:
        parent_dir = Path(args.dir)

    else:
        parent_dir = Path.cwd()

    if not parent_dir.exists():
        raise FileNotFoundError(str(parent_dir))

    if args.time is not None:

        dir_time = datetime.datetime.strptime(args.time, _file_datetime_format)
        dir_ = parent_dir / args.time

        if not dir_.exists():
            raise FileNotFoundError(str(dir_))

    else:

        dts = []

        for dir_ in parent_dir.iterdir():

            if dir_.is_dir():

                try:
                    dt = datetime.datetime.strptime(dir_.name, _file_datetime_format)

                except ValueError:
                    pass

                else:
                    dts.append(dt)

        dir_time = max(dts)
        dir_ = parent_dir / dir_time.strftime(_file_datetime_format)

    if args.pids is not None:
        pids = args.pids

    else:

        pids = []

        for pid_file in dir_.iterdir():

            if pid_file.is_file() and re.match(r'^\d+\.txt$', pid_file.name) is not None:
                pids.append(pid_file.stem)


    memory = args.memory
    print(f'm = {memory}, pids = {pids}, d1 = {d1}, d2 = {d2}, f = {parent_dir}, t = {dir_time}, dir = {dir_}')

    if d1 is None:

        for pid in pids:

            print(pid)
            pid_file = dir_ / f'{pid}.txt'
            to_print = []

            with pid_file.open('r') as fh:

                for i, line in enumerate(fh.readlines()):

                    i += 1

                    if memory > 0 and len(to_print) < memory:
                        to_print.append(f'\t{i:08d}, {line.strip()}')

                    elif memory > 0:
                        break

                    elif memory < 0:

                        if len(to_print) == -memory:
                            del to_print[0]

                        to_print.append(f'\t{i:08d}, {line.strip()}')

            for line in to_print:
                print(line)

    else:

        for pid in pids:

            print(pid)
            pid_file = dir_ / f'{pid}.txt'
            to_print = []
            stats = []
            start_time = None

            with pid_file.open('r') as fh:

                for i, line in enumerate(fh.readlines()):

                    dt, msg = _separate(line)

                    if msg == d1 and start_time is None:
                        start_time = dt

                    elif msg == d2 and start_time is not None:

                        stats.append((dir_time - start_time).total_seconds())
                        start_time = None

            if len(stats) > 0:
                print(
                    pid, statistics.mean(stats), len(stats), sum(stats), statistics.stdev(stats), min(stats),
                    [str(t) for t in statistics.quantiles(stats, n = 8)], max(stats)
                )

elif args.command == 'delete':

    regs = _load_regs(args.shorthands, args.idents, args.dir)
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

else:
    raise NotImplementedError
