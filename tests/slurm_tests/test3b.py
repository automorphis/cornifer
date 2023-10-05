import multiprocessing
import os
import sys
from pathlib import Path

from cornifer import ApriInfo, load_shorthand, AposInfo, DataNotFoundError

def f(test_home_dir, j, num_processes):

    reg = load_shorthand("reg", test_home_dir)

    with reg.open() as reg:

        for i in range(j, num_apri, num_processes):
            reg.set_apos(ApriInfo(i = i), AposInfo(i = i + 1))

if __name__ == "__main__":

    num_processes = int(sys.argv[1])
    test_home_dir = Path(sys.argv[2])
    num_apri = int(sys.argv[3])
    tmp_filename = Path(os.environ['TMPDIR'])
    reg = load_shorthand("reg", test_home_dir)
    reg.set_tmp_dir(tmp_filename)
    reg.make_tmp_db()
    mp_ctx = multiprocessing.get_context("spawn")
    procs = []

    for j in range(num_processes):
        procs.append(mp_ctx.Process(target = f, args = (test_home_dir, j, num_processes)))

    for proc in procs:
        proc.start()

    for proc in procs:
        proc.join()

    reg.update_perm_db()

if __name__ == "__main__":

    saves_dir = Path(sys.argv[1])
    num_apri = int(sys.argv[2])
    slurm_array_task_max = int(sys.argv[3])
    slurm_array_task_id = int(sys.argv[4])
    reg = load_shorthand("reg", saves_dir)

    with reg.open() as reg:

        for i in range(slurm_array_task_id - 1, num_apri, slurm_array_task_max):

            # with (Path.home() / f"log{slurm_array_task_id}.txt").open("a") as fh:
            #     fh.write(f"a, {i}\n")

            reg.set_apos(ApriInfo(i = i), AposInfo(i = i + 1))

            # with (Path.home() / f"log{slurm_array_task_id}.txt").open("a") as fh:
            #     fh.write(f"b, {i}\n")

            # reg.apos(ApriInfo(i = i))

        reg._db.sync()

    # if slurm_array_task_id == 1:
    #
    #     with reg.open(readonly = True):
    #
    #         for i in range(num_apri):
    #
    #             querying = True
    #             num_queries = 1
    #
    #             while querying:
    #
    #                 time.sleep(0.5)
    #
    #                 try:
    #                     apos = str(reg.apos(ApriInfo(i = i)))
    #
    #                 except DataNotFoundError:
    #                     num_queries += 1
    #
    #                 else:
    #                     querying = False
    #
    #             with (Path.home() / f"log{slurm_array_task_id}.txt").open("a") as fh:
    #                 fh.write(f"{i}, {num_queries}\n")



