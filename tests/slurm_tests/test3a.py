import math
import multiprocessing
import os
import shutil
import sys
from pathlib import Path

from cornifer import ApriInfo, load_shorthand, Block

def f(test_home_dir, i, total_blks, num_processes):

    reg = load_shorthand("reg", test_home_dir)

    with reg.open() as reg:

        for blk_index in range(i, total_blks, num_processes):

            start_index = blk_index * blk_size
            stop_index = min((blk_index + 1) * blk_size, total_indices)
            seg = list(n ** 2 for n in range(start_index, stop_index))

            with Block(seg, apri, start_index) as blk:
                reg.add_disk_blk(blk)

if __name__ == "__main__":

    num_processes = int(sys.argv[1])
    test_home_dir = Path(sys.argv[2])
    blk_size = int(sys.argv[3])
    total_indices = int(sys.argv[4])
    test_home_reg = load_shorthand("reg", test_home_dir)
    tmp_filename = Path(os.environ['TMPDIR'])
    total_blks = math.ceil(total_indices / blk_size)
    apri = ApriInfo(hi = "hello")
    reg = load_shorthand("reg", test_home_dir)
    reg.set_tmp_dir(tmp_filename)
    reg.make_tmp_dir()
    mp_ctx = multiprocessing.get_context("spawn")
    procs = []

    for i in range(num_processes):
        procs.append(mp_ctx.Process(target = f, args = (test_home_dir, i, total_blks, num_processes)))

    for proc in procs:
        proc.start()

    for proc in procs:
        proc.join()

    reg.update_perm_db()







