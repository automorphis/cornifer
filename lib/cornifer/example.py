from math import floor, sqrt
from pathlib import Path

from cornifer import Apri_Info, NumPy_Register, Block

my_saves_dir = Path.home() / "my_cornifer_saves"

def is_prime(m):

    if not isinstance(m, int) or m <= 1:
        return False
    for k in range( 2, floor(sqrt(m)) + 1 ):
        if m % k == 0:
            return False
    return True

lst = []
descr = Apri_Info(name ="primes")
blk = Block(lst, descr, 1)
register = NumPy_Register(my_saves_dir, "primes example")
register.add_ram_block(blk)

length = 100000
total_primes = 0
max_m = 10**9

with register.open() as register:

    for m in range(2, max_m+1):
        if is_prime(m):
            total_primes += 1
            lst.append(m)

        if (total_primes % length == 0 and total_primes > 0) or m == max_m:
            register.add_disk_block(blk)
            blk.set_start_n(total_primes+1)
            lst.clear()
