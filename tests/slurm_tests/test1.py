import math
import sys
from pathlib import Path

from cornifer import ApriInfo, load_shorthand, Block

if __name__ == "__main__":

    saves_dir = Path(sys.argv[1])
    blk_size = int(sys.argv[2])
    total_indices = int(sys.argv[3])
    slurm_array_task_max = int(sys.argv[4])
    slurm_array_task_id = int(sys.argv[5])
    reg = load_shorthand("reg", saves_dir)
    total_blks = math.ceil(total_indices / blk_size)
    apri = ApriInfo(hi = "hello")

    with reg.open() as reg:

        for blk_index in range(slurm_array_task_id - 1, total_blks, slurm_array_task_max):

            start_index = blk_index * blk_size
            stop_index = min((blk_index + 1) * blk_size, total_indices)
            seg = list(n ** 2 for n in range(start_index, stop_index))

            with Block(seg, apri, start_index) as blk:
                reg.add_disk_blk(blk)

            with (Path.home() / f"log{slurm_array_task_id}.txt").open("a") as fh:
                fh.write(f"{start_index}, {stop_index}\n")








