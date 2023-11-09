import re
import sys
from collections import OrderedDict
from pathlib import Path

def get_pid(line):

    pid_re = r'\d+\s'
    match = re.match(pid_re, line)
    if match is not None:
        return match[0][:-1]
    else:
        return None


if __name__ == '__main__':

    if len(sys.argv) == 2:
        memory = int(sys.argv[1])

    else:

        memory = 10
        pids = None

    if len(sys.argv) >= 3:
        pids = map(int, sys.argv[2:])


    last_lines = {}
    file = Path.home() / 'parallelize.txt'
    num_succeeded = 0

    with file.open('r') as fh:

        for i, line in enumerate(fh.readlines()):

            pid = get_pid(line)

            if pid is not None:

                if pid in last_lines.keys():

                    lines = last_lines[pid]

                    if len(lines) == memory:
                        del lines[0]

                    lines.append((i, line))

                else:
                    last_lines[pid] = [(i,line)]

                if 'succeeded' in line:
                    num_succeeded += 1

    for pid in last_lines.keys():

        if pids is None or pid in pids:

            print(pid)

            for val in last_lines[pid]:
                print(f"\t{val}")

    print(num_succeeded)
    print(num_succeeded // (len(last_lines) - 1))
