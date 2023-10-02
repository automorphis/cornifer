import time
from multiprocessing import get_context
from os import getpid


def f():
    print(getpid(), time.time() % 100, flush = True)
    time.sleep(3)
    print(getpid(), time.time() % 100, flush = True)

if __name__ == '__main__':

    ctx = get_context("spawn")

    for _ in range(4):
        p = ctx.Process(target = f)
        p.start()

