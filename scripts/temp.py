import argparse
import datetime
import re
import sys
from collections import OrderedDict
from pathlib import Path
from statistics import median


def get_pid(line):

    pid_re = r'\d+\s'
    match = re.match(pid_re, line)

    if match is not None:
        return match[0][:-1]

    else:
        return None

def get_time(line):

    time_re = r'\d{2}:\d{2}:\d{2}.\d{6}'
    match = re.match(time_re, line)

    if match is not None:
        return match[0][:-1], datetime.datetime.strptime(match[0][:-1], '%H:%M:%S.%f')

    else:
        return None, None

def separate(line):

    pid = get_pid(line)
    time_str, t = get_time(line)

    if pid is not None and time_str is not None:
        return pid, line[len(pid) : -len(time_str)].strip(), t

    else:
        return None, None, None

if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument('-m', '--memory', default = -10, type = int)
    parser.add_argument('-p', '--pids')
    parser.add_argument('-d1', '--diff1')
    parser.add_argument('-d2', '--diff2')
    args = parser.parse_args()
    memory = args.memory

    if args.pids is not None:
        pids = args.pids.split(' ')

    else:
        pids = None

    if (args.diff1 is None) != (args.diff2 is None):
        raise ValueError

    if args.diff1 is not None:
        d1, d2 = args.diff1, args.diff2

    else:
        d1 = d2 = None

    file = Path.home() / 'parallelize.txt'

    if d1 is None:

        print_lines = {}

        with file.open('r') as fh:

            for i, line in enumerate(fh.readlines()):

                pid = get_pid(line)

                if pid is not None:

                    if pid in print_lines.keys():

                        lines = print_lines[pid]

                        if memory > 0 and len(lines) < memory:
                            lines.append((i, line))

                        elif memory < 0:

                            if len(lines) == -memory:
                                del lines[0]

                            lines.append((i, line))

                    else:
                        print_lines[pid] = [(i, line)]

        for pid in print_lines.keys():

            if pids is None or pid in pids:

                print(pid)

                for val in print_lines[pid]:
                    print(f"\t{val}")

    else:

        stats = {}
        start_times = {}

        with file.open('r') as fh:

            for i, line in enumerate(fh.readlines()):

                pid, middle, t = separate(line)

                if pid is not None:

                    if pid not in stats.keys():

                        stats[pid] = []
                        start_times[pid] = None

                    start_time = start_times[pid]

                    if middle == d1 and start_time is None:
                        start_times[pid] = t

                    elif middle == d2 and start_time is not None:

                        stats[pid].append(t - start_time)
                        start_times[pid] = None

        for pid in stats.keys():

            if pids is None or pid in pids:
                print(pid, min(stats[pid]), median(stats[pid]), max(stats[pid]))