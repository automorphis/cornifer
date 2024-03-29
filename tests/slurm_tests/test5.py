import os
import sys
import time
from datetime import datetime
from pathlib import Path

import numpy as np

import cornifer.debug
from cornifer import NumpyRegister, ApriInfo, AposInfo, Block, _utilities
from cornifer.debug import log
from cornifer.multiprocessing import parallelize


def f(num_procs, proc_index, reg, num_apri, num_blks, blk_len):

    newline = '\n'

    try:

        with reg.open() as reg:

            for i in range(proc_index, num_apri, num_procs):

                apri = ApriInfo(i = i)
                reg.set_apos(apri, AposInfo(i = i + 1))

                for j in range(num_blks):

                    with Block(np.arange(j * blk_len, (j + 1) * blk_len), apri) as blk:
                        reg.append_disk_blk(blk, timeout = 10)

                log(f'{i} {reg.summary().replace(newline, " ")}')

    except BaseException as e:

        log(f'test5 error {repr(e)}')
        raise

if __name__ == "__main__":

    file = Path.home() / 'parallelize.txt'
    start = time.time()
    num_procs = int(sys.argv[1])
    test_home_dir = Path(sys.argv[2])
    num_apri = int(sys.argv[3])
    num_blks = int(sys.argv[4])
    blk_len = int(sys.argv[5])
    update_period = int(sys.argv[6])
    update_timeout = int(sys.argv[7])
    timeout = int(sys.argv[8])
    max_readers = int(sys.argv[9])
    tmp_filename = Path(os.environ['TMPDIR'])
    reg = NumpyRegister(test_home_dir, "sh", "msg", 2 ** 40, None, max_readers)

    with file.open('w') as fh:
        fh.write('')

    parallelize(num_procs, f, (reg, num_apri, num_blks, blk_len), timeout - 5, tmp_filename, update_period, update_timeout, 10)
