from pathlib import Path

from cornifer import Sequence_Description, NumPy_Register, Sequence

my_saves_dir = Path.home() / "my_cornifer_saves"

register = NumPy_Register(my_saves_dir, "primes example")

length = 100000

max_n = 10**9

def is_prime(n):pass

descr = Sequence_Description(msg = "primes")

lst = []
seq = Sequence(lst, descr, 1)
register.add_ram_sequence(seq)

total = 0

with register.open() as register:


    for n in range(2, max_n+1):

        if is_prime(n):
            total += 1
            lst.append(n)

        if (total % length == 0 and total > 0) or n == max_n:

            register.add_disk_sequence(seq)
            seq.set_start_n(total+1)
            lst.clear()
