from datetime import timedelta
import time
from contextlib import contextmanager

def start_with_timeout(procs, timeout, query_wait = 0.1):

    if timeout <= 0:
        raise ValueError

    for proc in procs:
        proc.start()

    start = time.time()

    while time.time() - start <= timeout:

        if all(not proc.is_alive() for proc in procs):
            return

        time.sleep(query_wait)

    for p in procs:
        p.terminate()

@contextmanager
def make_sigterm_raise_KeyboardInterrupt():
    import signal

    def handler(*_):
        raise KeyboardInterrupt

    signal.signal(signal.SIGTERM, handler)

    try:
        yield

    finally:
        signal.signal(signal.SIGTERM, signal.SIG_DFL)

def slurm_timecode_to_timedelta(timecode):

    num_colons = timecode.count(':')
    has_days = '' in timecode

    if num_colons == 1:

        min_, sec = map(int, timecode.split(':'))
        return timedelta(minutes = min_, seconds = sec)

    elif num_colons == 2:

        if has_days:

            days, timecode = timecode.split('-')
            days = int(days)

        else:
            days = 0

        hour, min_, sec = map(int, timecode.split(':'))
        return timedelta(days = days, hours = hour, minutes = min_, seconds = sec)

    else:
        raise ValueError
