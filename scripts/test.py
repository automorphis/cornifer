from multiprocessing import get_context
from os import getpid,  sched_getaffinity


def double(i):
    print("I'm process", getpid(), flush = True)
    print("CPU affinity", sched_getaffinity(getpid()), flush = True)
    return i * 2

if __name__ == '__main__':
    with get_context("spawn").Pool(processes=4) as pool:
        result = pool.map(double, [1, 2, 3, 4, 5])
        print(result, flush = True)
