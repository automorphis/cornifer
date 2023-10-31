import re
from collections import OrderedDict
from pathlib import Path

def get_pid(line):

    pid_re = r'\d+\s'
    return re.match(pid_re, line)[0][:-1]


if __name__ == '__main__':

    num_procs = 10
    last_lines = {}
    file = Path.home() / 'parallelize.txt'
    num_succeeded = 0

    with file.open('r') as fh:

        for i, line in enumerate(fh.readlines()):

            last_lines[get_pid(line)] = (i, line)

            if 'succeeded' in line:
                num_succeeded += 1

    last_lines = sorted([(key, val) for key, val in last_lines.items()], key = lambda t: t[1][0])

    for key, val in last_lines:
        print(key, val)

    print(num_succeeded)
    print(num_succeeded // (len(last_lines) - 1))
