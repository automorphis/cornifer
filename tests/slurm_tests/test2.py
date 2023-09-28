import sys
import time
from pathlib import Path

from cornifer import ApriInfo, load_shorthand, AposInfo, DataNotFoundError

if __name__ == "__main__":

    saves_dir = Path(sys.argv[1])
    num_apri = int(sys.argv[2])
    slurm_array_task_max = int(sys.argv[3])
    slurm_array_task_id = int(sys.argv[4])
    reg = load_shorthand("reg", saves_dir)

    with reg.open() as reg:

        for i in range(slurm_array_task_id - 1, num_apri, slurm_array_task_max):

            with (Path.home() / f"log{slurm_array_task_id}.txt").open("a") as fh:
                fh.write(f"a, {i}\n")

            reg.set_apos(ApriInfo(i = i), AposInfo(i = i + 1))

            with (Path.home() / f"log{slurm_array_task_id}.txt").open("a") as fh:
                fh.write(f"b, {i}\n")

            reg.apos(ApriInfo(i = i))

    if slurm_array_task_id == 1:

        with reg.open(readonly = True):

            for i in range(num_apri):

                querying = True
                num_queries = 1

                while querying:

                    time.sleep(0.5)

                    try:
                        apos = str(reg.apos(ApriInfo(i = i)))

                    except DataNotFoundError:
                        num_queries += 1

                    else:
                        querying = False

                with (Path.home() / f"log{slurm_array_task_id}.txt").open("a") as fh:
                    fh.write(f"{i}, {num_queries}\n")



