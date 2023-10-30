import multiprocessing
import os
import sys
import time
from datetime import datetime
from pathlib import Path

import numpy as np

from cornifer import NumpyRegister, ApriInfo, AposInfo, Block

def f(num_procs, proc_index, reg, num_apri, num_blks, blk_len):

    file = Path.home() / "parallelize.txt"

    with file.open('a') as fh:
        fh.write(f"{os.getpid()} \t starting {datetime.now().strftime('%H:%M:%S.%f')}\n")

    with reg.open() as reg:

        # with file.open('a') as fh:
        #     fh.write("2\n")

        for i in range(proc_index, num_apri, num_procs):

            with file.open('a') as fh:
                fh.write(f"{os.getpid()} {i} {datetime.now().strftime('%H:%M:%S.%f')}\n")

            apri = ApriInfo(i = i)
            reg.set_apos(apri, AposInfo(i = i + 1))

            for j in range(num_blks):
                # with file.open('a') as fh:
                #     fh.write(f"\t{os.getpid()} {j} {datetime.now().strftime('%H:%M:%S.%f')}\n")
                with Block(np.arange(j * blk_len, (j + 1) * blk_len), apri) as blk:
                    reg.append_disk_blk(blk)


if __name__ == "__main__":

    start = time.time()
    num_procs = int(sys.argv[1])
    test_home_dir = Path(sys.argv[2])
    num_apri = int(sys.argv[3])
    num_blks = int(sys.argv[4])
    blk_len = int(sys.argv[5])
    tmp_filename = Path(os.environ['TMPDIR'])
    reg = NumpyRegister(test_home_dir, "sh", "msg")
    mp_ctx = multiprocessing.get_context("spawn")
    reg._create_txn_shared_data(mp_ctx, num_procs, 15)
    procs = []

    for proc_index in range(num_procs):
        procs.append(mp_ctx.Process(target = f, args = (
            num_procs, proc_index, reg, num_apri, num_blks, blk_len
        )))

    with reg.tmp_db(tmp_filename):

        for proc in procs:
            proc.start()

        for proc in procs:
            proc.join()