import argparse
import datetime
import re
import sys
from collections import OrderedDict
from pathlib import Path
from statistics import median, quantiles, stdev, mean

file_datetime_format = '%m-%d-%Y-%H-%M-%S-%f'
pid_file_datetime_format = '%H-%M-%S-%f'
line_datetime_format = '%H:%M:%S.%f'

def separate(line):
    return line[15], datetime.datetime.strptime(line[:15], pid_file_datetime_format), line[16:].strip()

if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument('-m', '--memory', default = -10, type = int)
    parser.add_argument('-p', '--pids')
    parser.add_argument('-d1', '--diff1')
    parser.add_argument('-d2', '--diff2')
    parser.add_argument('-f', '--file')
    parser.add_argument('-t', '--time')
    args = parser.parse_args()
    memory = args.memory

    if args.diff1 is not None:
        d1, d2 = args.diff1, args.diff2

    else:
        d1 = d2 = None

    if args.file is not None:
        f = Path(args.file)

    else:
        f = Path.home() / 'debugs'

    if args.time is not None:
        t = datetime.datetime.strptime(args.time, file_datetime_format)

    else:

        dts = []

        for file in f.iterdir():

            if file.name[:6] == 'debug-':

                try:
                    dt = datetime.datetime.strptime(file.name[6:], file_datetime_format)

                except ValueError:
                    pass

                else:
                    dts.append(dt)

        t = max(dts)

    file = f / f'debug-{t.strftime(file_datetime_format)}'

    if args.pids is not None:
        pids = args.pids.split(',')

    else:

        pids = []

        for pid_file in file.iterdir():

            if re.match(r'^debug\d+\.txt$', pid_file.name) is not None:
                pids.append(pid_file.name[5:-4])


    if (args.diff1 is None) != (args.diff2 is None):
        raise ValueError

    print(f'm = {memory}, pids = {pids}, d1 = {d1}, d2 = {d2}, f = {f}, t = {t}, file = {file}')

    # for pid in pids:
    #
    #     with (file / f'debug{pid}.txt').open('r') as fh:
    #
    #         print(file / f'debug{pid}.txt')
    #         print(fh.readline())

    if d1 is None:

        for pid in pids:

            print(pid)
            pid_file = file / f'debug{pid}.txt'
            to_print = []

            with pid_file.open('r') as fh:

                for i, line in enumerate(fh.readlines()):

                    if memory > 0 and len(to_print) < memory:
                        to_print.append(f'\t{i + 1 : 08d}, {line.strip()}')

                    elif memory > 0:
                        break

                    elif memory < 0:

                        if len(to_print) == -memory:
                            del to_print[0]

                        to_print.append(f'\t{i + 1 : 08d}, {line.strip()}')

            for line in to_print:
                print(line)

    else:

        for pid in pids:

            print(pid)
            pid_file = file / f'debug{pid}.txt'
            to_print = []
            stats = []
            start_time = None

            with pid_file.open('r') as fh:

                for i, line in enumerate(fh.readlines()):

                    _, dt, msg = separate(line)

                    if msg == d1 and start_time is None:
                        start_time = dt

                    elif msg == d2 and start_time is not None:

                        stats.append((t - start_time).total_seconds())
                        start_time = None

            if len(stats) > 0:
                print(pid, mean(stats), len(stats), sum(stats), stdev(stats), min(stats), [str(t) for t in quantiles(stats, n = 8)], max(stats))
