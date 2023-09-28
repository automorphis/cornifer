import datetime
import re
import shutil
import unittest
import subprocess
from pathlib import Path
import time

from cornifer import NumpyRegister, ApriInfo, AposInfo

saves_dir = Path.home() / "cornifer_slurm_testcases"
python_command = "sage -python"
error_filename = saves_dir / 'test_slurm_error.txt'
test_filename = saves_dir / 'test.sbatch'
slurm_tests_filename = Path(__file__).parent / "slurm_tests"
allocation_query_sec = 0.5
running_query_sec = 0.5
allocation_max_sec = 60
total_indices = 10050
num_apri = 100

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

def write_batch_file(batch_filename, time_sec, slurm_task_array_max, slurm_test_main_filename, args, output):

    with batch_filename.open("w") as fh:
        fh.write(
f"""#!/usr/bin/env bash

#SBATCH --job-name=corniferslurmtests
#SBATCH --time={datetime.timedelta(seconds = time_sec)}
#SBATCH --ntasks=1
#SBATCH --ntasks-per-core=1
#SBATCH --error={error_filename}
{'#SBATCH --output=/dev/null' if not output else ''}
#SBATCH --array=1-{slurm_task_array_max}

srun {python_command} {slurm_test_main_filename} {saves_dir} {args} $SLURM_ARRAY_TASK_MAX $SLURM_ARRAY_TASK_ID
""")

class TestSlurm(unittest.TestCase):

    def setUp(self):
        self.job_id = None

    def tearDown(self):

        subprocess.run(["scancel", self.job_id])
        time.sleep(2)

    @classmethod
    def setUpClass(cls):

        if saves_dir.exists():
            shutil.rmtree(saves_dir)

        saves_dir.mkdir(parents=True, exist_ok=False)
        cls.reg = NumpyRegister(saves_dir, "reg", "msg", 2 ** 40)

        with cls.reg.open(): pass

    @classmethod
    def tearDownClass(cls):

        if saves_dir.exists():
            shutil.rmtree(saves_dir)

    def check_empty_error_file(self):

        error_filename.exists()

        with error_filename.open("r") as fh:

            contents = ""

            for line in fh:
                contents += line

        if len(contents) > 0:
            self.fail(f"Must be empty error file! Contents: {contents}")

    def check_timeout_error_file(self, num_timouts):

        error_filename.exists()

        with error_filename.open("r") as fh:

            contents = ""

            for line in fh:
                contents += line

        if len(re.findall(r".*CANCELLED AT.*DUE TO TIME LIMIT.*", contents)) != num_timouts:
            self.fail(f"Invalid error file. Contents: {contents}")


    def test_slurm_1(self):

        reg = type(self).reg
        slurm_test_main_filename = slurm_tests_filename / 'test1.py'
        running_max_sec = 15
        blk_size = 100
        slurm_time = running_max_sec + 1
        apri = ApriInfo(hi = "hello")
        slurm_array_task_max = 10
        write_batch_file(
            test_filename, slurm_time, slurm_array_task_max, slurm_test_main_filename,
            f"{blk_size} {total_indices}",
            False
        )
        print("Submitting test batch #1...")
        job_id = submit_batch(test_filename)
        self.job_id = job_id
        wait_till_running(job_id, allocation_max_sec, allocation_query_sec)
        print("Running test #1...")
        wait_till_not_running(job_id, running_max_sec, running_query_sec)
        print("Checking test #1...")
        self.check_empty_error_file()

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

    def test_slurm_2(self):

        reg = type(self).reg
        slurm_test_main_filename = slurm_tests_filename / 'test2.py'
        running_max_sec = 600
        slurm_time = running_max_sec + 1
        slurm_array_task_max = 2
        write_batch_file(
            test_filename, slurm_time, slurm_array_task_max, slurm_test_main_filename,
            str(num_apri),
            False
        )
        print("Submitting test batch #2...")
        job_id = submit_batch(test_filename)
        wait_till_running(job_id, allocation_max_sec, allocation_query_sec)
        print("Running test #2...")
        wait_till_not_running(job_id, running_max_sec, running_query_sec)
        print("Checking test #2...")
        self.check_empty_error_file()

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
                list(reg[ApriInfo(hi = "hello"), :])
            )

    def test_slurm_3(self):
        # this one is forced to crash due to low time limit
        # (The writer of `cornifer.registers.Register.set_apos` will sleep for a long time)
        reg = type(self).reg
        slurm_test_main_filename = slurm_tests_filename / 'test3.py'
        running_max_sec = 60
        slurm_time = running_max_sec + 1
        slurm_array_task_max = 7
        write_batch_file(
            test_filename, slurm_time, slurm_array_task_max, slurm_test_main_filename,
            str(num_apri),
            True
        )
        print("Submitting test batch #3...")
        job_id = submit_batch(test_filename)
        wait_till_running(job_id, allocation_max_sec, allocation_query_sec)
        print("Running test #3...")
        time.sleep(slurm_time + 1)
        print("Checking test #3...")
        self.check_timeout_error_file(0)

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

                if i % slurm_array_task_max == 0 and i >= 2 * slurm_array_task_max:
                    self.assertEqual(
                        AposInfo(i = i + 1),
                        reg.apos(apri)
                    )

                else:
                    self.assertEqual(
                        AposInfo(i = i + 2),
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
                list(reg[ApriInfo(hi = "hello"), :])
            )

    def test_slurm_4(self):
        # this one is forced to crash due to low time limit
        # (The reader of `cornifer.registers.Register.set_apos` will sleep for a long time)
        reg = type(self).reg
        slurm_test_main_filename = slurm_tests_filename / 'test3.py'
        running_max_sec = 60
        slurm_time = running_max_sec + 1
        slurm_array_task_max = 5
        write_batch_file(
            test_filename, slurm_time, slurm_array_task_max, slurm_test_main_filename, str(num_apri),
            False
        )
        print("Submitting test batch #4...")
        job_id = submit_batch(test_filename)
        wait_till_running(job_id, allocation_max_sec, allocation_query_sec)
        print("Running test #4...")
        time.sleep(slurm_time + 1)
        print("Checking test #4...")
        self.check_timeout_error_file(0)

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

                if i % slurm_array_task_max == 1 and i >= 10 * slurm_array_task_max:
                    self.assertEqual(
                        AposInfo(i = i + 2),
                        reg.apos(apri)
                    )

                else:
                    self.assertEqual(
                        AposInfo(i = i + 3),
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
                list(reg[ApriInfo(hi = "hello"), :])
            )


