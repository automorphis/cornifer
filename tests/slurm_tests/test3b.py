import multiprocessing
import os
import sys
from pathlib import Path

from cornifer import ApriInfo, load_shorthand, AposInfo, DataNotFoundError

def f(test_home_dir, j, num_apri, num_processes):

    reg = load_shorthand("reg", test_home_dir)

    with reg.open() as reg:

        for i in range(j, num_apri, num_processes):
            reg.set_apos(ApriInfo(i = i), AposInfo(i = i + 1))

if __name__ == "__main__":

    num_processes = int(sys.argv[1])
    test_home_dir = Path(sys.argv[2])
    num_apri = int(sys.argv[3])
    tmp_filename = Path(os.environ['TMPDIR'])
    reg = load_shorthand("reg", test_home_dir)
    reg.set_tmp_dir(tmp_filename)
    reg.make_tmp_db()
    mp_ctx = multiprocessing.get_context("spawn")
    procs = []

    for j in range(num_processes):
        procs.append(mp_ctx.Process(target = f, args = (test_home_dir, j, num_apri, num_processes)))

    for proc in procs:
        proc.start()

    for proc in procs:
        proc.join()

    reg.update_perm_db()
    reg.set_tmp_dir(reg.dir)