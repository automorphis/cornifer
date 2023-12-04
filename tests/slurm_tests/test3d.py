import multiprocessing
import os
import sys
import time
from pathlib import Path

from cornifer import ApriInfo, load, AposInfo
import cornifer
from cornifer._utilities.multiprocessing import start_with_timeout

def f(test_home_dir, j, num_apri, num_processes):

    reg = load("reg", test_home_dir)
    with reg.open() as reg:

        for i in range(j, num_apri, num_processes):

            if j == 1 and i == 5 * num_processes + 1:
                cornifer.registers._debug = 2

            reg.set_apos(ApriInfo(i = i), AposInfo(i = i + 3), exists_ok = True)

            if j == 1 and i == 5 * num_processes + 1:
                cornifer.registers._debug = 0

if __name__ == "__main__":

    start = time.time()
    num_processes = int(sys.argv[1])
    test_home_dir = Path(sys.argv[2])
    num_apri = int(sys.argv[3])
    timeout = int(sys.argv[4])
    tmp_filename = Path(os.environ['TMPDIR'])
    reg = load("reg", test_home_dir)
    mp_ctx = multiprocessing.get_context("spawn")
    procs = []

    with reg.tmp_db(tmp_filename) as reg:

        for j in range(num_processes):
            procs.append(mp_ctx.Process(target = f, args = (test_home_dir, j, num_apri, num_processes)))

        start_with_timeout(procs, timeout + start - time.time())

        for proc in procs:
            proc.join()
