import sys
from pathlib import Path

import lmdb

if __name__ == "__main__":

    saves_dir = Path(sys.argv[1])
    filename = saves_dir / sys.argv[2]
    num_entries = int(sys.argv[3])
    slurm_array_task_max = int(sys.argv[4])
    slurm_array_task_id = int(sys.argv[5])
    db = lmdb.open(str(filename))

    with (Path.home() / f"log{slurm_array_task_id}.txt").open("w") as fh:

        for i in range(slurm_array_task_id - 1, num_entries, slurm_array_task_max):

            with db.begin(write = True) as rw_txn:

                i = str(i).encode("ASCII")
                rw_txn.put(i, i)
                fh.write(i.decode("ASCII") + "\n")

        db.close()
