import sys
from pathlib import Path

from cornifer._utilities.lmdb import open_lmdb

if __name__ == "__main__":

    saves_dir = Path(sys.argv[1])
    filename = saves_dir / sys.argv[2]
    num_entries = int(sys.argv[3])
    slurm_array_task_max = int(sys.argv[4])
    slurm_array_task_id = int(sys.argv[5])
    db = open_lmdb(filename, 2 ** 40, False)

    with db.begin(write = True) as rw_txn:

        for i in range(slurm_array_task_id - 1, num_entries, slurm_array_task_max):

            i = str(i).encode("ASCII")
            rw_txn.put(i, i)

    db.close()


