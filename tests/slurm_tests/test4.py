import math
import sys
import time
from pathlib import Path

from cornifer import ApriInfo, load_shorthand, AposInfo, DataNotFoundError
import cornifer

if __name__ == "__main__":

    saves_dir = Path(sys.argv[1])
    num_apri = int(sys.argv[2])
    slurm_array_task_max = int(sys.argv[3])
    slurm_array_task_id = int(sys.argv[4])
    reg = load_shorthand("reg", saves_dir)
    query_sec = 0.5

    if slurm_array_task_id == 1:

        querying = True

        with reg.open(readonly = True) as reg:

            while querying:

                time.sleep(query_sec)

                for i in range(num_apri):

                    if i % slurm_array_task_max != 0:

                        try:
                            reg.apos(ApriInfo(i = i))

                        except DataNotFoundError:
                            break # i loop

                else:
                    querying = False

    with reg.open() as reg:

        for i in range(slurm_array_task_id - 1, num_apri, slurm_array_task_max):

            if slurm_array_task_id == 1 and i == 2 * slurm_array_task_max:
                cornifer.registers._debug = 1

            reg.set_apos(ApriInfo(i = i), AposInfo(i = i + 2), exists_ok = True)

            if slurm_array_task_id == 1 and i == 2 * slurm_array_task_max:
                cornifer.registers._debug = 0

