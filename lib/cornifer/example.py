from math import floor, sqrt
from pathlib import Path

from cornifer import ApriInfo, NumpyRegister, Block

my_saves_dir = Path.home() / "my_cornifer_saves"

def is_prime(m):

    if not isinstance(m, int) or m <= 1:
        return False
    for k in range( 2, floor(sqrt(m)) + 1 ):
        if m % k == 0:
            return False
    return True

lst = []
descr = ApriInfo(name ="primes")
blk = Block(lst, descr, 1)
register = NumpyRegister(my_saves_dir, "primes example")
register.addRamBlk(blk)

length = 100000
total_primes = 0
max_m = 10**9

with register.open() as register:

    for m in range(2, max_m+1):
        if is_prime(m):
            total_primes += 1
            lst.append(m)

        if (total_primes % length == 0 and total_primes > 0) or m == max_m:
            register.addDiskBlk(blk)
            blk.setStartN(total_primes + 1)
            lst.clear()
