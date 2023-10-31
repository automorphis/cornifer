import re
from pathlib import Path

def get_pid(line):

    pid_re = r'\d+\s'
    return re.match(pid_re, line)[0][:-1]


if __name__ == '__main__':

    num_procs = 10
    last_lines = {}
    file = Path.home() / 'parallelize.txt'

    with file.open('r') as fh:

        for line in fh.readlines():

            last_lines[get_pid(line)] = line

            if len(last_lines) >= num_procs:
                break # line loop

    for key, val in last_lines:
        print(key, val)