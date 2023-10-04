import multiprocessing
import os
import random
import time
from contextlib import contextmanager, ExitStack
# SuperFastPython.com
# example of a mutual exclusion (mutex) lock for processes




@contextmanager
def acquire(lock, block, timeout = None):

    release = lock.acquire(block, timeout)

    try:
        yield

    finally:

        if release:
            lock.release()

# # work function
# def task(lock, identifier, value):
#     # acquire the lock
#     with acquire(lock, True):
#         print(f'>process {identifier} got the lock, sleeping for {value}')
#         time.sleep(value)
#         print(f'>process {identifier} release')
#
#
# # entry point
# if __name__ == '__main__':
#     # create the shared lock
#     mp_ctx = multiprocessing.get_context("spawn")
#     lock = mp_ctx.Lock()
#     # create a number of processes with different sleep times
#     processes = [mp_ctx.Process(target=task, args=(lock, i, random.random())) for i in range(10)]
#     # start the processes
#     for process in processes:
#         process.start()
#     # wait for all processes to finish
#     for process in processes:
#         process.join()

# def f(lock, sleep_sec):
#
#     lock.acquire(block = False)
#
#     try:
#
#         print(1, os.getpid(), sleep_sec)
#         time.sleep(sleep_sec)
#
#     finally:
#         lock.release()
#
# if __name__ == "__main__":
#
#     num_procs = 2
#     mp_ctx = multiprocessing.get_context("spawn")
#     lock = mp_ctx.Lock()
#     procs = []
#
#     for _ in range(num_procs):
#
#         sleep_sec = random.uniform(0.5, 1.5)
#         args = (lock, sleep_sec)
#         procs.append(mp_ctx.Process(target = f, args = args))
#
#     for proc in procs:
#         proc.start()
#
#     for proc in procs:
#         proc.join()
#

def reader(num_active_readers, num_waiting_writers, reader_cond, writer_cond, max_active_readers):

    start = time.time()
    sleep_sec = random.uniform(0, 1)

    with reader_cond:

        while num_active_readers.value >= max_active_readers and num_waiting_writers != 0:
            reader_cond.wait()

        with num_active_readers.get_lock():
            num_active_readers.value += 1

    delay = time.time() - start

    try:

        print("enter reader", os.getpid(), num_active_readers.value, delay)
        time.sleep(sleep_sec)
        print("exit reader", os.getpid(), num_active_readers.value, sleep_sec)

    finally:

        with num_active_readers.get_lock():
            num_active_readers.value -= 1

        if num_waiting_writers.value == 0:

            with reader_cond:
                reader_cond.notify()

        else:

            with writer_cond:
                writer_cond.notify()

# def reader(num_active_readers, reader_cond, max_active_readers, sleep_sec):
#
#     start = time.time()
#
#     with reader_cond:
#
#         while num_active_readers.value >= max_active_readers:
#             reader_cond.wait()
#
#
#     with num_active_readers.get_lock():
#         num_active_readers.value += 1
#
#     delay = time.time() - start
#
#     try:
#
#         print("enter", os.getpid(), num_active_readers.value - 1, delay)
#         time.sleep(sleep_sec)
#         print("exit", os.getpid())
#
#     finally:
#
#         with num_active_readers.get_lock():
#             num_active_readers.value -= 1
#
#     with reader_cond:
#         reader_cond.notify()

def writer(num_active_readers, num_waiting_writers, reader_cond, writer_cond, max_active_readers):

    start = time.time()
    sleep_sec = random.uniform(0, 1)

    with writer_cond:

        with num_waiting_writers.get_lock():
            num_waiting_writers.value += 1

        while num_active_readers.value > 0:
            writer_cond.wait()

        with num_waiting_writers.get_lock():
            num_waiting_writers.value -= 1

        delay = time.time() - start

        try:

            print("enter writer", os.getpid(), num_active_readers.value, delay)
            time.sleep(sleep_sec)
            print("exit writer", os.getpid(), num_active_readers.value, sleep_sec)

        finally:

            if num_waiting_writers.value == 0:

                with reader_cond:
                    reader_cond.notify()

            else:

                with writer_cond:
                    writer_cond.notify()



if __name__ == '__main__':

    num_procs = 20
    mp_ctx = multiprocessing.get_context("spawn")
    reader_cond = mp_ctx.Condition()
    writer_cond = mp_ctx.Condition()
    num_active_readers = mp_ctx.Value("i", 0)
    num_waiting_writers = mp_ctx.Value("i", 0)
    max_active_readers = 7
    procs = []

    for _ in range(num_procs):

        args = (num_active_readers, num_waiting_writers, reader_cond, writer_cond, max_active_readers)

        if random.randint(0, 1) == 0:
            procs.append(mp_ctx.Process(target = reader, args = args))

        else:
            procs.append(mp_ctx.Process(target = writer, args = args))

    for proc in procs:

        time.sleep(random.uniform(0, 0.25))
        proc.start()

    for proc in procs:
        proc.join()


