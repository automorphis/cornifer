import os
import sys
from pathlib import Path
import multiprocessing

from cornifer._utilities.lmdb import open_lmdb

def f(db_filename, num_entries, num_processes, proc_id):

    db = open_lmdb(db_filename, 2 ** 40, False)

    with db.begin(write = True) as rw_txn:

        for i in range(proc_id, num_entries, num_processes):

            i = str(i).encode("ASCII")
            rw_txn.put(i, i)

    db.close()

if __name__ == "__main__":

    saves_dir = Path(os.environ['TMPDIR'])
    num_processes = int(sys.argv[1])
    db_filename = saves_dir / sys.argv[2]
    num_entries = int(sys.argv[3])
    ctx = multiprocessing.get_context("spawn")

    for proc_id in range(num_processes):

        p = ctx.Process(target = f, args = (db_filename, num_entries, num_processes, proc_id))
        p.start()