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

    if len(sys.argv) >= 2:
        memory = int(sys.argv[1])

    else:

        memory = -10
        pids = None

    if len(sys.argv) >= 3:
        pids = sys.argv[2:]


    last_lines = {}
    file = Path.home() / 'parallelize.txt'

    with file.open('r') as fh:

        for i, line in enumerate(fh.readlines()):

            pid = get_pid(line)

            if pid is not None:

                if pid in last_lines.keys():

                    if memory > 0 and len(lines) < memory:
                        lines.append((i, line))

                    elif memory < 0:

                        lines = last_lines[pid]

                        if len(lines) == -memory:
                            del lines[0]

                        lines.append((i, line))

                else:
                    last_lines[pid] = [(i,line)]

    for pid in last_lines.keys():

        if pids is None or pid in pids:

            print(pid)

            for val in last_lines[pid]:
                print(f"\t{val}")
