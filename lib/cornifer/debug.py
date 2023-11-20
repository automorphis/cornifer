import argparse
import os
import datetime
import re
import statistics
from pathlib import Path

from ._utilities import check_return_Path, check_type

__all__ = [
    'init_dir',
    'set_dir',
    'log'
]
_log_file = None
_file_datetime_format = '%Y-%m-%d-%H-%M-%S-%f'
_line_datetime_format = '%H:%M:%S.%f'
_line_datetime_len = 2 + 1 + 2 + 1 + 2 + 1 + 6

def init_dir(parent_dir):

    parent_dir = check_return_Path(parent_dir, 'parent_dir')
    dir_ = parent_dir / datetime.datetime.now().strftime(_file_datetime_format)
    dir_.mkdir()
    set_dir(dir_)
    return dir_

def set_dir(dir_):

    dir_ = check_return_Path(dir_, 'dir_')
    global _log_file
    _log_file = dir_ / f'{os.getpid()}.txt'

def log(message):

    check_type(message, 'message', str)

    if _log_file is not None:

        with _log_file.open('a') as fh:
            fh.write(f'{datetime.datetime.now().strftime("%H:%M:%S.%f")} {message}\n')

def _separate(line):
    return (
        datetime.datetime.strptime(line[:_line_datetime_len], _line_datetime_format),
        line[_line_datetime_len + 1:].strip()
    )

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

    if (args.diff1 is None) != (args.diff2 is None):
        raise ValueError

    if args.diff1 is not None:
        d1, d2 = args.diff1, args.diff2

    else:
        d1 = d2 = None

    if args.file is not None:
        parent_dir = Path(args.file)

    else:
        parent_dir = Path.cwd()

    if not parent_dir.exists():
        raise FileNotFoundError(str(parent_dir))

    if args.time is not None:

        dir_time = datetime.datetime.strptime(args.time, _file_datetime_format)
        dir_ = parent_dir / args.time

        if not dir_.exists():
            raise FileNotFoundError(str(dir_))

    else:

        dts = []

        for dir_ in parent_dir.iterdir():

            if dir_.is_dir():

                try:
                    dt = datetime.datetime.strptime(dir_.name, _file_datetime_format)

                except ValueError:
                    pass

                else:
                    dts.append(dt)

        dir_time = max(dts)
        dir_ = parent_dir / dir_time.strftime(_file_datetime_format)

    if args.pids is not None:
        pids = args.pids.split(',')

    else:

        pids = []

        for pid_file in dir_.iterdir():

            if pid_file.is_file() and re.match(r'^\d+\.txt$', pid_file.name) is not None:
                pids.append(pid_file.stem)


    print(f'm = {memory}, pids = {pids}, d1 = {d1}, d2 = {d2}, f = {parent_dir}, t = {dir_time}, dir = {dir_}')

    if d1 is None:

        for pid in pids:

            print(pid)
            pid_file = dir_ / f'{pid}.txt'
            to_print = []

            with pid_file.open('r') as fh:

                for i, line in enumerate(fh.readlines()):

                    i += 1

                    if memory > 0 and len(to_print) < memory:
                        to_print.append(f'\t{i:08d}, {line.strip()}')

                    elif memory > 0:
                        break

                    elif memory < 0:

                        if len(to_print) == -memory:
                            del to_print[0]

                        to_print.append(f'\t{i:08d}, {line.strip()}')

            for line in to_print:
                print(line)

    else:

        for pid in pids:

            print(pid)
            pid_file = dir_ / f'{pid}.txt'
            to_print = []
            stats = []
            start_time = None

            with pid_file.open('r') as fh:

                for i, line in enumerate(fh.readlines()):

                    dt, msg = _separate(line)

                    if msg == d1 and start_time is None:
                        start_time = dt

                    elif msg == d2 and start_time is not None:

                        stats.append((dir_time - start_time).total_seconds())
                        start_time = None

            if len(stats) > 0:
                print(
                    pid, statistics.mean(stats), len(stats), sum(stats), statistics.stdev(stats), min(stats),
                    [str(t) for t in statistics.quantiles(stats, n = 8)], max(stats)
                )
