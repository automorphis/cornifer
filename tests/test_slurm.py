import datetime
import os
import re
import shutil
import unittest
import subprocess
from pathlib import Path
import time

import lmdb

from cornifer import NumpyRegister, ApriInfo, AposInfo
from cornifer._utilities import random_unique_filename
from cornifer._utilities.lmdb import open_lmdb, r_txn_prefix_iter

test_home_dir = Path.home() / "cornifer_slurm_testcases"
python_command = "sage -python"
error_filename = test_home_dir / 'test_slurm_error.txt'
sbatch_filename = test_home_dir / 'test.sbatch'
slurm_tests_filename = Path(__file__).parent / "slurm_tests"
allocation_query_sec = 0.5
running_query_sec = 0.5
allocation_max_sec = 60
total_indices = 10050
timeout_extra_wait_sec = 90
num_apri = 100
subprocess._USE_VFORK = False
subprocess._USE_POSIX_SPAWN = True

def write_batch_file(time_sec, slurm_test_main_filename, num_processes, args):

    with sbatch_filename.open("w") as fh:
        fh.write(
f"""#!/usr/bin/env bash

#SBATCH --job-name=corniferslurmtests
#SBATCH --time={datetime.timedelta(seconds = time_sec)}
#SBATCH --ntasks={num_processes}
#SBATCH --ntasks-per-core=1
#SBATCH --error={error_filename}

{python_command} {slurm_test_main_filename} {num_processes} {test_home_dir} {args}
""")

class TestSlurm(unittest.TestCase):

    def setUp(self):
        self.job_id = None

    def tearDown(self):

        subprocess.run(["scancel", self.job_id])
        time.sleep(2)

    @classmethod
    def setUpClass(cls):

        if test_home_dir.exists():
            shutil.rmtree(test_home_dir)

        test_home_dir.mkdir(parents = True, exist_ok = False)

    @classmethod
    def tearDownClass(cls):

        if test_home_dir.exists():
            shutil.rmtree(test_home_dir)

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

        if len(re.findall(r"STEP.*CANCELLED AT.*DUE TO TIME LIMIT.*", contents)) != num_timouts:
            self.fail(f"Invalid error file. Contents: {contents}")

    def wait_till_running(self, max_sec, query_sec):

        querying = True
        start = time.time()

        while querying:

            if time.time() - start >= max_sec + timeout_extra_wait_sec:
                raise Exception("Ran out of time!")

            time.sleep(query_sec)
            squeue_process = subprocess.run(
                ["squeue", "-j", self.job_id, "-o", "%.2t"], capture_output = True, text = True
            )
            querying = "PD" in squeue_process.stdout

    def wait_till_not_running(self, max_sec, query_sec):

        querying = True
        start = time.time()

        while querying:

            if time.time() - start >= max_sec:
                raise Exception("Ran out of time!")

            time.sleep(query_sec)
            squeue_process = subprocess.run(
                ["squeue", "-j", self.job_id, "-o", "%.2t"], capture_output = True, text = True
            )
            querying = squeue_process.stdout != "ST\n"

        time.sleep(query_sec)

    def submit_batch(self, batch_filename):

        sbatch_process = subprocess.run(
            ["sbatch", str(batch_filename)], capture_output = True, text = True
        )
        self.job_id = sbatch_process.stdout[20:-1]

    def test_slurm_1(self):

        slurm_test_main_filename = slurm_tests_filename / 'test1.py'
        num_entries = 10000
        running_max_sec = 100
        slurm_time = running_max_sec + 1
        num_processes = 2
        db_filename = "lmdb"
        write_batch_file(slurm_time, slurm_test_main_filename, num_processes, f"{db_filename} {num_entries}")
        print("Submitting test batch #1...")
        self.submit_batch(sbatch_filename)
        self.wait_till_running(allocation_max_sec, allocation_query_sec)
        print(f"Running test #1 (running_max_sec = {running_max_sec})...")
        self.wait_till_not_running(running_max_sec, running_query_sec)
        print("Checking test #1...")
        self.check_empty_error_file()
        db = lmdb.open(str(test_home_dir / db_filename))

        with db.begin() as ro_txn:

            for i in range(num_entries):

                i = str(i).encode("ASCII")
                self.assertEqual(
                    ro_txn.get(i),
                    i
                )

            with r_txn_prefix_iter(b"", ro_txn) as it:
                total = sum(1 for _ in it)

            self.assertEqual(
                total,
                num_entries
            )

        db.close()

    def test_slurm_2(self):

        slurm_test_main_filename = slurm_tests_filename / 'test2.py'
        running_max_sec = 100
        slurm_time = running_max_sec + 1
        num_processes = 2
        db_filename = "lmdb"

        for num_entries in [1, 5, 10, 50, 100, 500, 1000]:

            write_batch_file(slurm_time, slurm_test_main_filename, num_processes, f"{db_filename} {num_entries}")
            print(f"Submitting test batch #2 (num_entries = {num_entries})...")
            self.submit_batch(sbatch_filename)
            self.wait_till_running(allocation_max_sec, allocation_query_sec)
            print(f"Running test #2 (running_max_sec = {running_max_sec}) (num_entries = {num_entries})...")
            self.wait_till_not_running(running_max_sec, running_query_sec)
            print(f"Checking test #2 (num_entries = {num_entries})...")
            self.check_empty_error_file()
            db = lmdb.open(str(test_home_dir / db_filename))

            for i in range(num_entries):

                with db.begin() as ro_txn:

                    i = str(i).encode("ASCII")
                    self.assertEqual(
                        ro_txn.get(i),
                        i
                    )

            with db.begin() as ro_txn:

                with r_txn_prefix_iter(b"", ro_txn) as it:
                    total = sum(1 for _ in it)

            self.assertEqual(
                total,
                num_entries
            )
            db.close()
            shutil.rmtree(test_home_dir / db_filename)

    def test_slurm_3(self):

        slurm_test_main_filename = slurm_tests_filename / 'test3a.py'
        running_max_sec = 40
        blk_size = 100
        slurm_time = running_max_sec + 1
        apri = ApriInfo(hi = "hello")
        num_processes = 1
        reg = NumpyRegister(test_home_dir, "reg", "hi")
        write_batch_file(slurm_time, slurm_test_main_filename, num_processes, f"{blk_size} {total_indices}")
        print(1, list(test_home_dir.iterdir()))
        print("Submitting test batch #3a...")
        self.submit_batch(sbatch_filename)
        print(2, list(test_home_dir.iterdir()))
        self.wait_till_running(allocation_max_sec, allocation_query_sec)
        print(3, list(test_home_dir.iterdir()))
        print(f"Running test #3a (running_max_sec = {running_max_sec})...")
        self.wait_till_not_running(running_max_sec, running_query_sec)
        print(4, list(test_home_dir.iterdir()))
        print("Checking test #3a...")
        print(5, list(test_home_dir.iterdir()))
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

    def test_slurm_4(self):

        reg = type(self).reg
        slurm_test_main_filename = slurm_tests_filename / 'test3b.py'
        running_max_sec = 80
        slurm_time = running_max_sec + 1
        slurm_array_task_max = 2
        write_batch_file(slurm_time, slurm_array_task_max, slurm_test_main_filename, str(num_apri))
        print("Submitting test batch #4...")
        self.submit_batch(sbatch_filename)
        self.wait_till_running(allocation_max_sec, allocation_query_sec)
        print(f"Running test #4 (running_max_sec = {running_max_sec})...")
        self.wait_till_not_running(running_max_sec, running_query_sec)
        print("Checking test #4...")
        self.check_empty_error_file()

        with reg.open(readonly = True):

            ret = []

            for i in range(num_apri):

                if ApriInfo(i = i) not in reg:
                    ret.append(ApriInfo(i = i))

            print(ret)


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

    def test_slurm_5(self):
        # this one is forced to crash due to low time limit
        # (The writer of `cornifer.registers.Register.set_apos` will sleep for a long time)
        reg = type(self).reg
        slurm_test_main_filename = slurm_tests_filename / 'test3c.py'
        running_max_sec = 20
        slurm_time = running_max_sec + 1
        slurm_array_task_max = 7
        write_batch_file(slurm_time, slurm_array_task_max, slurm_test_main_filename, str(num_apri))
        print("Submitting test batch #5...")
        self.submit_batch(sbatch_filename)
        self.wait_till_running(allocation_max_sec, allocation_query_sec)
        print(f"Running test #5 (running_max_sec = {running_max_sec})...")
        time.sleep(slurm_time + timeout_extra_wait_sec)
        print("Checking test #5...")
        self.check_timeout_error_file(1)

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

    def test_slurm_6(self):
        # this one is forced to crash due to low time limit
        # (The reader of `cornifer.registers.Register.set_apos` will sleep for a long time)
        reg = type(self).reg
        slurm_test_main_filename = slurm_tests_filename / 'test3d.py'
        running_max_sec = 60
        slurm_time = running_max_sec + 1
        slurm_array_task_max = 5
        write_batch_file(slurm_time, slurm_array_task_max, slurm_test_main_filename, str(num_apri))
        print("Submitting test batch #6...")
        self.submit_batch(sbatch_filename)
        self.wait_till_running(allocation_max_sec, allocation_query_sec)
        print(f"Running test #6 (running_max_sec = {running_max_sec})...")
        time.sleep(slurm_time + timeout_extra_wait_sec)
        print("Checking test #6...")
        self.check_timeout_error_file(1)

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


