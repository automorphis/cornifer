import math
import sys
from pathlib import Path

from cornifer import ApriInfo, load_shorthand, AposInfo
import cornifer

if __name__ == "__main__":

    saves_dir = Path(sys.argv[1])
    num_apri = int(sys.argv[2])
    slurm_array_task_max = int(sys.argv[3])
    slurm_array_task_id = int(sys.argv[4])
    reg = load_shorthand("reg", saves_dir)

    with reg.open() as reg:

        for i in range(slurm_array_task_id - 1, num_apri, slurm_array_task_max):


            if slurm_array_task_id == 1 and i == 2 * slurm_array_task_max:
                cornifer.registers._debug = 1

            with (Path.home() / f"log{slurm_array_task_id}.txt").open("a") as fh:
                fh.write(f"{cornifer.registers._debug}, {slurm_array_task_id}, {i}, {ApriInfo(i = i)}, {AposInfo(i = i + 2)}\n")

            reg.set_apos(ApriInfo(i = i), AposInfo(i = i + 2), exists_ok = True)

            with (Path.home() / f"log{slurm_array_task_id}.txt").open("a") as fh:
                fh.write(f"{cornifer.registers._debug}, {slurm_array_task_id}, {i}, {ApriInfo(i = i)}, {AposInfo(i = i + 2)}\n")

            if slurm_array_task_id == 1 and i == 2 * slurm_array_task_max:
                cornifer.registers._debug = 0

