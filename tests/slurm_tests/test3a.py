import math
import multiprocessing
import os
import shutil
import sys
from pathlib import Path

from cornifer import ApriInfo, load_shorthand, Block

if __name__ == "__main__":

    num_processes = int(sys.argv[1])
    test_home_dir = Path(sys.argv[2])
    blk_size = int(sys.argv[3])
    total_indices = int(sys.argv[4])
    test_home_reg = load_shorthand("reg", test_home_dir)
    tmp_filename = Path(os.environ['TMPDIR'])
    total_blks = math.ceil(total_indices / blk_size)
    apri = ApriInfo(hi = "hello")
    mp_ctx = multiprocessing.get_context("spawn")
    procs = []

    with reg.open() as reg:

        for blk_index in range(slurm_array_task_id - 1, total_blks, slurm_array_task_max):

            start_index = blk_index * blk_size
            stop_index = min((blk_index + 1) * blk_size, total_indices)
            seg = list(n ** 2 for n in range(start_index, stop_index))

            with Block(seg, apri, start_index) as blk:
                reg.add_disk_blk(blk)







