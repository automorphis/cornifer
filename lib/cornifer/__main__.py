import argparse
from pathlib import Path

from .errors import CannotLoadError
from .registers import Register

def add_shorthand_ident_arguments(parser):

    parser.add_argument('-i', '--ident', help = 'Flags a register identifier.', metavar = 'ID', nargs = 1)
    parser.add_argument('shorthands', help = 'List of register shorthands.', metavar = 'SH', nargs = '*')

def add_save_dir_argument(parser):
    parser.add_argument(
        '-d', '--dir', help = 'Register(s) parent directory (default: current directory)', default = Path.cwd(),
        type = Path
    )

parser = argparse.ArgumentParser(
    prog = 'Cornifer',
    description = 'Command line utility for Cornifer.'
)
subparsers = parser.add_subparsers()

parser_summary = subparsers.add_parser('summary', help = 'Print register summaries.')
add_save_dir_argument(parser)
add_shorthand_ident_arguments(parser_summary)

parser_debug = subparsers.add_parser('debug', help = hi)
parser_debug.add_argument('-m', '--memory', default = -10, type = int)
parser_debug.add_argument('-p', '--pids')
parser_debug.add_argument('-d1', '--diff1')
parser_debug.add_argument('-d2', '--diff2')
parser_debug.add_argument('-f', '--file')
parser_debug.add_argument('-t', '--time')

parser_search = subparsers.add_parser('search', help = hi)
parser_delete = subparsers.add_parser('delete', help = hi)
parser_move = subparsers.add_parser('move', help = hi)
parser_copy = subparsers.add_parser('copy', help = hi)
parser_merge = subparsers.add_parser('merge', help = hi)
parser_help = subparsers.add_parser('help', help = hi)
to_print = f'`{str(filename)}` contains the following Registers:\n\n'

for d in filename.iterdir():

    try:
        to_print += Register._summary(d) + '\n\n'

    except CannotLoadError:
        pass

print(to_print)
