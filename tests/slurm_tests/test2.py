import multiprocessing
import os
import shutil
import subprocess
import sys
from pathlib import Path

import lmdb

from cornifer._utilities.lmdb import r_txn_prefix_iter


def f(db_filename, num_entries, num_processes, proc_id):

    db = lmdb.open(str(db_filename))

    for i in range(proc_id, num_entries, num_processes):

        with db.begin(write = True) as rw_txn:

            i = str(i).encode("ASCII")
            rw_txn.put(i, i)

    db.close()

if __name__ == "__main__":

    num_processes = int(sys.argv[1])
    test_home_dir = Path(sys.argv[2])
    db_filepath = Path(os.environ['TMPDIR']) / sys.argv[3]
    num_entries = int(sys.argv[4])

    if db_filepath.exists():
        shutil.rmtree(db_filepath)

    db_filepath.mkdir(parents = False, exist_ok = False)
    db = lmdb.open(str(db_filepath), map_size = 2 ** 40, subdir = True, readonly = False, create = False)
    db.close()
    mp_ctx = multiprocessing.get_context("spawn")
    procs = []

    for i in range(num_processes):
        procs.append(mp_ctx.Process(target = f, args = (db_filepath, num_entries, num_processes, i)))

    for proc in procs:
        proc.start()

    for proc in procs:
        proc.join()

    df_process = subprocess.run(['df' , '-T', os.environ['TMPDIR']], capture_output = True, text = True)
    print(df_process.stdout)
    db = lmdb.open(str(db_filepath))

    with db.begin() as ro_txn:

        for i in range(num_entries):

            i = str(i).encode("ASCII")
            assert ro_txn.get(i) == i
            print(ro_txn.get(i))

        with r_txn_prefix_iter(b"", ro_txn) as it:
            total = sum(1 for _ in it)

        assert total == num_entries

    (test_home_dir / db_filepath.name).mkdir(exist_ok = False)
    print((test_home_dir / db_filepath.name).exists())
    db.copy(str(test_home_dir / db_filepath.name), compact = True)
    print(test_home_dir / db_filepath.name)
    print((test_home_dir / db_filepath.name).exists())
    print(list((test_home_dir / db_filepath.name).iterdir()))
    db.close()
    shutil.rmtree(db_filepath)
    print((test_home_dir / db_filepath.name).exists())
    print(list((test_home_dir / db_filepath.name).iterdir()))
