from multiprocessing import get_context
from os import getpid


def f():
    print("I'm process", getpid(), flush = True)

if __name__ == '__main__':

    ctx = get_context("spawn")

    for _ in range(4):
        p = ctx.Process(target = f)
        p.start()

