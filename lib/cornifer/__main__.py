import argparse
from pathlib import Path

from .regloader import load_ident

parser = argparse.ArgumentParser(
    prog='Cornifer',
    description='Prints information about Registers in the given directory.'
)
parser.add_argument('filename', default = Path.cwd())
args = parser.parse_args()
filename = Path(args.filename)
to_print = f'`{str(filename)}` contains the following Registers:\n\n'

for d in filename.iterdir():

    try:
        reg = load_ident(d)

    except FileNotFoundError:
        pass

    else:

        with reg.open(readonly=True) as reg:
            to_print += reg.summary() + "\n\n"

print(to_print)
