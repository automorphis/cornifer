from pathlib import Path

from cornifer import Sequence_Description, NumPy_Register, Sequence

my_saves_dir = Path.home() / "my_cornifer_saves"

register = NumPy_Register(my_saves_dir, "primes example")

length = 100000

max_n = 10**9

def is_prime(n):pass

descr = Sequence_Description(msg = "primes")

with register.open() as register:

    total = 0
    primes = []
    seq = Sequence(primes, descr, 1)
    register.add_ram_seq(seq)

    for n in range(2, max_n+1):

        if is_prime(n):
            total += 1
            primes.append(n)

        if (total % length == 0 and total > 0) or n == max_n:

            register.add_disk_seq(seq)

            primes = []
            seq = Sequence(primes, descr, total + 1)
            register.add_ram_seq(seq)

descr = Sequence_Description(msg = "primes")
register = cornify(descr)

with register.open() as register:

    for p in register[descr, :]:
        print(p)

