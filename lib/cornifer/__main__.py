import argparse
from pathlib import Path

from .errors import CannotLoadError
from .registers import Register

parser = argparse.ArgumentParser(
    prog = 'Cornifer',
    description = 'Command line utility for Cornifer.'
)
parser.add_argument(
    'filename', help = 'Register save directory (default: current working directory)', default = Path.cwd(), nargs = '?'
)
parser.add_argument('-i', '--info', help = 'Print ')
parser.add_argument('-h', '--help', help = 'Display this help message.', action = 'store_true')
parser.add_argument('-r', '--remove', help = 'Delete registers.', action = 'store_true')
parser.add_argument('-s', '--shorthand', help = 'Comma separated list of shorthands (default: all registers).')
parser.add_argument('-d', '--ident', help = 'Comma separated list of identifiers.')
parser.add_argument(
    '-m', '--move', help = 'Move to given save directory (default: current working directory)', default = Path.cwd()
)
parser.add_argument(
    '-c', '--copy', help = 'Copy to given save directory (default: current working directory)', default = Path.cwd()
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
