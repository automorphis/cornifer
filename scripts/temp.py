import re
from collections import OrderedDict
from pathlib import Path

def get_pid(line):

    pid_re = r'\d+\s'
    return re.match(pid_re, line)[0][:-1]


if __name__ == '__main__':

    num_procs = 10
    last_lines = OrderedDict()
    file = Path.home() / 'parallelize.txt'

    with file.open('r') as fh:

        for i, line in enumerate(fh.readlines()):

            last_lines[i, get_pid(line)] = line

            if len(last_lines) >= num_procs:
                break # line loop

    for key, val in last_lines.items():
        print(key, val)