import argparse
import datetime
import re
import statistics
from pathlib import Path

from .errors import CannotLoadError
from .registers import Register
from .debug import _file_datetime_format, _line_datetime_format, _line_datetime_len


def _add_shorthand_ident_arguments(parser):

    parser.add_argument('-i', '--ident', help = 'Flags a register identifier.', metavar = 'ID', nargs = '+')
    parser.add_argument('shorthands', help = 'List of register shorthands.', metavar = 'SH', nargs = '*')

def _add_save_dir_argument(parser):
    parser.add_argument(
        '-d', '--dir', help = 'Register(s) parent directory (default: current directory)', default = Path.cwd(),
        type = Path
    )

def _add_target_dir_argument(parser):
    parser.add_argument('-t', '--target', help = "Target directory.", nargs = 1, required = True, type = Path)

def _separate(line):
    return (
        datetime.datetime.strptime(line[:_line_datetime_len], _line_datetime_format),
        line[_line_datetime_len + 1:].strip()
    )

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

# parser_delete = subparsers.add_parser('delete', help = 'Delete registers.')
# add_save_dir_argument(parser_delete)
# add_shorthand_ident_arguments(parser_delete)
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

    to_print = f'`{str(args.dir)}` contains the following Registers:\n\n'

    for d in args.dir.iterdir():

        try:
            to_print += Register._summary(d) + '\n\n'

        except CannotLoadError:
            pass

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
        pids = args.pids.split(',')

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

else:
    raise NotImplementedError
