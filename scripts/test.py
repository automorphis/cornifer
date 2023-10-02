from multiprocessing import Pool
from os import getpid, cpu_count

def double(i):
    print("I'm process", getpid(), flush = True)
    return i * 2

if __name__ == '__main__':
    with Pool() as pool:
        result = pool.map(double, [1, 2, 3, 4, 5])
        print("Number of CPUs:", cpu_count())
        print(result, flush = True)
