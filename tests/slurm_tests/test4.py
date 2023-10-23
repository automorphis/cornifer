import os
import sys
import time
from pathlib import Path

import numpy as np

from cornifer import parallelize, NumpyRegister, ApriInfo, AposInfo, Block

def f(num_procs, proc_index, reg, num_apri, num_blks, blk_len):

    with reg.open() as reg:

        for i in range(proc_index, num_apri, num_procs):

            apri = ApriInfo(i = i)
            reg.set_apos(apri, AposInfo(i = i + 1))

            for j in range(num_blks):

                with Block(np.arange(j * blk_len, (j + 1) * blk_len), apri) as blk:
                    reg.append_disk_blk(blk)

if __name__ == "__main__":

    start = time.time()
    num_procs = int(sys.argv[1])
    test_home_dir = Path(sys.argv[2])
    num_apri = int(sys.argv[3])
    num_blks = int(sys.argv[4])
    blk_len = int(sys.argv[5])
    update_period = int(sys.argv[6])
    update_timeout = int(sys.argv[7])
    timeout = int(sys.argv[8])
    tmp_filename = Path(os.environ['TMPDIR'])
    reg = NumpyRegister(test_home_dir, "sh", "msg")
    parallelize(num_procs, f, (reg, num_apri, num_blks, blk_len), timeout - 5, tmp_filename, (reg,), update_period, update_timeout)