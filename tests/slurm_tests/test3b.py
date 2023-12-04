import multiprocessing
import os
import sys
from pathlib import Path

from cornifer import ApriInfo, load, AposInfo, DataNotFoundError
from cornifer._utilities.multiprocessing import start_with_timeout


def f(test_home_dir, j, num_apri, num_processes):

    reg = load("reg", test_home_dir)

    with reg.open() as reg:

        for i in range(j, num_apri, num_processes):
            reg.set_apos(ApriInfo(i = i), AposInfo(i = i + 1))

if __name__ == "__main__":

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

        start_with_timeout(procs, timeout)

        for proc in procs:
            proc.join()