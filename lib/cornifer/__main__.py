import argparse
from pathlib import Path

from .errors import CannotLoadError
from .registers import Register

parser = argparse.ArgumentParser(
    prog = 'Cornifer',
    description = 'Prints information about Registers in the given directory.'
)
parser.add_argument('filename', nargs = '?', default = Path.cwd())
args = parser.parse_args()
filename = Path(args.filename)
to_print = f'`{str(filename)}` contains the following Registers:\n\n'

for d in filename.iterdir():

    try:
        to_print += Register._summary(d) + '\n\n'

    except CannotLoadError:
        pass

print(to_print)
