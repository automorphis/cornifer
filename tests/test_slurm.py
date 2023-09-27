import datetime
import shutil
import unittest
import subprocess
from pathlib import Path
import time

from cornifer import NumpyRegister, ApriInfo, AposInfo

saves_dir = Path.home() / "cornifer_slurm_testcases"
python_command = "sage -python"

def submit_batch(batch_filename):

    sbatch_process = subprocess.run(["sbatch", str(batch_filename)], capture_output = True, text = True)
    return sbatch_process.stdout[20:-1]

def wait_till_running(job_id, max_sec, query_sec):

    querying = True
    start = time.time()

    while querying:

        if time.time() - start >= max_sec:
            raise Exception("Ran out of time!")

        time.sleep(query_sec)
        squeue_process = subprocess.run(["squeue", "-j", job_id, "-o", "%.2t"], capture_output = True, text = True)
        querying = "PD" in squeue_process.stdout

def wait_till_not_running(job_id, max_sec, query_sec):

    querying = True
    start = time.time()

    while querying:

        if time.time() - start >= max_sec:
            raise Exception("Ran out of time!")

        time.sleep(query_sec)
        squeue_process = subprocess.run(["squeue", "-j", job_id, "-o", "%.2t"], capture_output=True, text=True)
        querying = squeue_process.stdout != "ST\n"

class TestSlurm(unittest.TestCase):

    def setUp(self):

        if saves_dir.exists():
            shutil.rmtree(saves_dir)

        saves_dir.mkdir(parents=True, exist_ok=False)

    def tearDown(self):

        if saves_dir.exists():
            shutil.rmtree(saves_dir)

    def test_slurm(self):

        error_filename = saves_dir / 'test_slurm_error.txt'
        sbatch_header = (
f"""#!/usr/bin/env bash

#SBATCH --job-name=corniferslurmtests
#SBATCH --time={{0}}
#SBATCH --ntasks=1
#SBATCH --ntasks-per-core=1
#SBATCH --error={error_filename}
#SBATCH --array=1-{{1}}

""")
        test_filename = saves_dir / 'test.sbatch'
        reg = NumpyRegister(saves_dir, "reg", "msg", 2 ** 40)
        allocation_query_sec = 0.5
        running_query_sec = 0.5
        allocation_max_sec = 60

        slurm_test_main_filename = Path(__file__).parent / 'slurm_test_main1.py'
        running_max_sec = 15
        blk_size = 100
        total_indices = 10050
        slurm_time = running_max_sec + 1
        apri = ApriInfo(hi = "hello")
        slurm_array_task_max = 10

        with reg.open(): pass

        with test_filename.open("w") as fh:
            fh.write(
                sbatch_header.format(datetime.timedelta(seconds = slurm_time), slurm_array_task_max) +
                f"srun {python_command} {slurm_test_main_filename} {saves_dir} {blk_size} {total_indices} "
                f"$SLURM_ARRAY_TASK_MAX $SLURM_ARRAY_TASK_ID"
            )

        print("Submitting test batch #1...")
        job_id = submit_batch(test_filename)
        wait_till_running(job_id, allocation_max_sec, allocation_query_sec)
        print("Running test #1...")
        wait_till_not_running(job_id, running_max_sec, running_query_sec)
        print("Checking test #1...")
        self.assertTrue(error_filename.exists())

        with error_filename.open("r") as fh:
            for _ in fh:
                self.fail("Must be empty error file!")

        with reg.open(readonly = True):

            self.assertIn(
                apri,
                reg
            )
            self.assertEqual(
                1,
                reg.num_apri()
            )
            self.assertEqual(
                total_indices,
                reg.total_len(apri)
            )
            self.assertEqual(
                [n ** 2 for n in range(total_indices)],
                list(reg[apri, :])
            )

        slurm_test_main_filename = Path(__file__).parent / 'slurm_test_main2.py'
        running_max_sec = 600
        num_apri = 100000
        slurm_time = running_max_sec + 1
        apri = ApriInfo(hi = "hello")
        slurm_array_task_max = 2

        with test_filename.open("w") as fh:
            fh.write(
                sbatch_header.format(datetime.timedelta(seconds = slurm_time), slurm_array_task_max) +
                f"srun {python_command} {slurm_test_main_filename} {saves_dir} {num_apri} "
                f"$SLURM_ARRAY_TASK_MAX $SLURM_ARRAY_TASK_ID"
            )

        print("Submitting test batch #1...")
        job_id = submit_batch(test_filename)
        wait_till_running(job_id, allocation_max_sec, allocation_query_sec)
        print("Running test #1...")
        wait_till_not_running(job_id, running_max_sec, running_query_sec)
        print("Checking test #1...")
        self.assertTrue(error_filename.exists())

        with error_filename.open("r") as fh:

            contents = ""

            for line in fh:
                contents += line

        if len(contents) > 0:
            self.fail(f"Must be empty error file! Contents: {contents}")

        with reg.open(readonly = True):

            for i in range(num_apri):

                apri = ApriInfo(i = i)
                self.assertIn(
                    apri,
                    reg
                )
                self.assertEqual(
                    0,
                    reg.num_blks(apri)
                )
                self.assertEqual(
                    AposInfo(i = i + 1),
                    reg.apos(apri)
                )

            self.assertIn(
                ApriInfo(hi = "hello"),
                reg
            )
            self.assertEqual(
                num_apri + 1,
                reg.num_apri()
            )
            self.assertEqual(
                total_indices,
                reg.total_len(ApriInfo(hi = "hello"))
            )
            self.assertEqual(
                [n ** 2 for n in range(total_indices)],
                list(reg[apri, :])
            )

