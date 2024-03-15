import os
import datetime

from ._utilities import check_return_Path, check_type

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

    print(message)