import sys
import time
from pathlib import Path

from cornifer import ApriInfo, load_shorthand, AposInfo

if __name__ == "__main__":

    saves_dir = Path(sys.argv[1])
    num_apri = int(sys.argv[2])
    slurm_array_task_max = int(sys.argv[3])
    slurm_array_task_id = int(sys.argv[4])
    reg = load_shorthand("reg", saves_dir)

    with reg.open() as reg:

        for i in range(slurm_array_task_id - 1, num_apri, slurm_array_task_max):

            with (Path.home() / f"log{slurm_array_task_id}.txt").open("a") as fh:
                fh.write(f"1, {i}\n")

            reg.set_apos(ApriInfo(i = i), AposInfo(i = i + 1))

            with (Path.home() / f"log{slurm_array_task_id}.txt").open("a") as fh:
                fh.write(f"2, {i}\n")

            reg.apos(ApriInfo(i = i))

    if slurm_array_task_id == 1:

        time.sleep(5)

        with reg.open(readonly = True):

            with (Path.home() / f"log{slurm_array_task_id}.txt").open("a") as fh:

                for i in range(num_apri):
                    fh.write(str(reg.apos(ApriInfo(i = i))) + "\n")



