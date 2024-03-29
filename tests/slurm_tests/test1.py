import os
import shutil
import sys
from pathlib import Path
import multiprocessing

import lmdb

from cornifer._utilities.lmdb import  r_txn_prefix_iter


def f(db_filename, num_entries, num_processes, proc_id):

    db = lmdb.open(str(db_filename))

    with db.begin(write = True) as rw_txn:

        for i in range(proc_id, num_entries, num_processes):

            i = str(i).encode("ASCII")
            rw_txn.put(i, i)

    db.close()

if __name__ == "__main__":

    num_processes = int(sys.argv[1])
    test_home_dir = Path(sys.argv[2])
    db_filename = Path(os.environ['TMPDIR']) / sys.argv[3]
    num_entries = int(sys.argv[4])

    if db_filename.exists():
        shutil.rmtree(db_filename)

    db_filename.mkdir(parents = False, exist_ok = False)
    db = lmdb.open(str(db_filename), map_size = 2 ** 40, subdir = True, readonly = False, create = False)
    db.close()
    mp_ctx = multiprocessing.get_context("spawn")
    procs = []

    for i in range(num_processes):
        procs.append(mp_ctx.Process(target = f, args = (db_filename, num_entries, num_processes, i)))

    for proc in procs:
        proc.start()

    for proc in procs:
        proc.join()

    db = lmdb.open(str(db_filename))

    for i in range(num_entries):

        with db.begin() as ro_txn:

            i = str(i).encode("ASCII")
            assert ro_txn.get(i) == i

    with db.begin() as ro_txn:

        with r_txn_prefix_iter(b"", ro_txn) as it:
            total = sum(1 for _ in it)

    assert total == num_entries

    db.close()
    shutil.move(db_filename, test_home_dir / db_filename.name)
