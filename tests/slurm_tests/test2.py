import math
import sys
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
                fh.write(f"1, {i}")

            reg.set_apos(ApriInfo(i = i), AposInfo(i = i + 1))

            with (Path.home() / f"log{slurm_array_task_id}.txt").open("a") as fh:
                fh.write(f"2, {i}")
