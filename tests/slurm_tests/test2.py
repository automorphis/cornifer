import multiprocessing
import sys
from pathlib import Path

import lmdb


def f(db_filename, num_entries, num_processes, proc_id):

    db = lmdb.open(str(db_filename))

    for i in range(proc_id, num_entries, num_processes):

        with db.begin(write = True) as rw_txn:

            i = str(i).encode("ASCII")
            rw_txn.put(i, i)

    db.close()

if __name__ == "__main__":

    saves_dir = Path(sys.argv[1])
    num_processes = int(sys.argv[2])
    db_filename = saves_dir / sys.argv[3]
    num_entries = int(sys.argv[4])
    ctx = multiprocessing.get_context("spawn")

    for proc_id in range(num_processes):

        p = ctx.Process(target = f, args = (db_filename, num_entries, num_processes, proc_id))
        p.start()
