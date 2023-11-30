import argparse
from pathlib import Path

from .errors import CannotLoadError
from .registers import Register

parser = argparse.ArgumentParser(
    prog = 'Cornifer',
    description = 'Command line utility for Cornifer.'
)
parser.add_argument(
    'command', help = 'summary (default), search, delete, move, copy, merge, help', default = 'summary', nargs = '?',
    choices = ('summary', 'search', 'delete', 'move', 'copy', 'merge', 'help')
)
parser.add_argument(
    '-d', '--dir', help = 'Register save directory (default: current working directory)', default = Path.cwd(),
    nargs = '?', type = Path
)
parser.add_argument('-s', '--shorthand', help = 'Comma separated list of shorthands (default: all registers).')
parser.add_argument('-i', '--ident', help = 'Comma separated list of identifiers.')
parser.add_argument(
    '-m', '--move', help = 'Move to given save directory (default: current working directory)', default = Path.cwd(),
    type = Path
)
parser.add_argument(
    '-c', '--copy', help = 'Copy to given save directory (default: current working directory)', default = Path.cwd(),
    type = Path
)
args = parser.parse_args()
filename = Path(args.filename)
to_print = f'`{str(filename)}` contains the following Registers:\n\n'

for d in filename.iterdir():

    try:
        to_print += Register._summary(d) + '\n\n'

    except CannotLoadError:
        pass

print(to_print)
