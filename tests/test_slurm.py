import datetime
import os
import re
import shutil
import unittest
import subprocess
from pathlib import Path
import time

import lmdb
import numpy as np

from cornifer import NumpyRegister, ApriInfo, AposInfo, load_shorthand, Block
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
timeout_extra_wait_sec = 30

def write_batch_file(time_sec, slurm_test_main_filename, num_processes, args):

    with sbatch_filename.open("w") as fh:
        fh.write(
f"""#!/usr/bin/env bash

#SBATCH --job-name=corniferslurmtests
#SBATCH --time={datetime.timedelta(seconds = time_sec)}
#SBATCH --ntasks={num_processes}
#SBATCH --nodes=1
#SBATCH --ntasks-per-core=1
#SBATCH --error={error_filename}

{python_command} {slurm_test_main_filename} {num_processes} {test_home_dir} {args}
""")
#SBATCH --output=/dev/null
#SBATCH --mail-user=lane.662@osu.edu
#SBATCH --mail-type=all

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

    def check_timeout_error_file(self):

        error_filename.exists()

        with error_filename.open("r") as fh:

            contents = ""

            for line in fh:
                contents += line

        if re.match(r"^slurmstepd: error: \*\*\* JOB.*ON.*CANCELLED AT.*DUE TO TIME LIMIT \*\*\*$", contents) is None:
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

    def submit_batch(self):

        sbatch_process = subprocess.run(
            ["sbatch", str(sbatch_filename)], capture_output = True, text = True
        )
        self.job_id = sbatch_process.stdout[20:-1]
        print(self.job_id)

    def test_slurm_1(self):

        slurm_test_main_filename = slurm_tests_filename / 'test1.py'
        num_entries = 10000
        running_max_sec = 100
        slurm_time = running_max_sec + 1
        num_processes = 10
        db_filename = "lmdb"
        write_batch_file(slurm_time, slurm_test_main_filename, num_processes, f"{db_filename} {num_entries}")
        print("Submitting test batch #1...")
        self.submit_batch()
        self.wait_till_running(allocation_max_sec, allocation_query_sec)
        print(f"Running test #1 (running_max_sec = {running_max_sec})...")
        self.wait_till_not_running(running_max_sec, running_query_sec)
        print("Checking test #1...")
        self.check_empty_error_file()
        self.assertTrue((test_home_dir / db_filename).exists())
        db = lmdb.open(str(test_home_dir / db_filename))

        try:

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

        finally:

            db.close()
            shutil.rmtree(test_home_dir / db_filename)

    def test_slurm_2(self):

        slurm_test_main_filename = slurm_tests_filename / 'test2.py'
        running_max_sec = 100
        slurm_time = running_max_sec + 1
        num_processes = 7
        db_filename = "lmdb"

        for num_entries in [1, 5, 10, 50, 100, 500, 1000]:

            write_batch_file(slurm_time, slurm_test_main_filename, num_processes, f"{db_filename} {num_entries}")
            print(f"Submitting test batch #2 (num_entries = {num_entries})...")
            self.submit_batch()
            self.wait_till_running(allocation_max_sec, allocation_query_sec)
            print(f"Running test #2 (running_max_sec = {running_max_sec}) (num_entries = {num_entries})...")
            self.wait_till_not_running(running_max_sec, running_query_sec)
            print(f"Checking test #2 (num_entries = {num_entries})...")
            self.check_empty_error_file()
            self.assertTrue(test_home_dir.exists())
            start = time.time()
            max_num_queries = 100

            for _ in range(max_num_queries):

                try:
                    self.assertTrue((test_home_dir / db_filename).exists())

                except AssertionError:
                    time.sleep(0.5)

                else:
                    break

            else:
                raise AssertionError

            print(time.time() - start)
            db = lmdb.open(str(test_home_dir / db_filename))

            try:


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

            finally:

                db.close()
                shutil.rmtree(test_home_dir / db_filename)

    def test_slurm_3(self):

        slurm_test_main_filename = slurm_tests_filename / 'test3a.py'
        running_max_sec = 40
        blk_size = 100
        slurm_time = running_max_sec + 1
        apri = ApriInfo(hi = "hello")
        num_processes = 10
        total_indices = 10050
        reg = NumpyRegister(test_home_dir, "reg", "hi")
        write_batch_file(slurm_time, slurm_test_main_filename, num_processes, f"{blk_size} {total_indices} {slurm_time - 10}")
        print("Submitting test batch #3a...")
        self.submit_batch()
        self.wait_till_running(allocation_max_sec, allocation_query_sec)
        print(f"Running test #3a (running_max_sec = {running_max_sec})...")
        self.wait_till_not_running(running_max_sec, running_query_sec)
        print("Checking test #3a...")
        self.check_empty_error_file()
        reg = load_shorthand("reg", test_home_dir, True)

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

        slurm_test_main_filename = slurm_tests_filename / 'test3b.py'
        running_max_sec = 80
        slurm_time = running_max_sec + 1
        num_processes = 2
        num_apri = 100
        write_batch_file(slurm_time, slurm_test_main_filename, num_processes, f"{num_apri} {slurm_time - 10}")
        print("Submitting test batch #3b...")
        self.submit_batch()
        self.wait_till_running(allocation_max_sec, allocation_query_sec)
        print(f"Running test #3b (running_max_sec = {running_max_sec})...")
        self.wait_till_not_running(running_max_sec, running_query_sec)
        print("Checking test #3b...")
        self.check_empty_error_file()
        reg = load_shorthand("reg", test_home_dir, True)

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
        # this one is forced to crash due to low time limit
        # (The writer of `cornifer.registers.Register.set_apos` will sleep for a long time)
        slurm_test_main_filename = slurm_tests_filename / 'test3c.py'
        running_max_sec = 15
        slurm_time = running_max_sec + 1
        num_processes = 7
        write_batch_file(slurm_time, slurm_test_main_filename, num_processes, f"{num_apri} {slurm_time - 10}")
        print("Submitting test batch #3c...")
        self.submit_batch()
        self.wait_till_running(allocation_max_sec, allocation_query_sec)
        print(f"Running test #3c (running_max_sec = {running_max_sec})...")
        time.sleep(slurm_time + timeout_extra_wait_sec + 10)
        print("Checking test #3c...")
        self.check_empty_error_file()
        stall_indices = [None] * num_processes
        stall_indices[1] = 2 * num_processes + 1
        reg = load_shorthand("reg", test_home_dir, True)

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
                stall_index = stall_indices[i % num_processes]

                if stall_index is not None and i >= stall_index:
                    self.assertEqual(
                        AposInfo(i = i + 1),
                        reg.apos(apri)
                    )

                elif stall_index is not None:
                    self.assertEqual(
                        AposInfo(i = i + 2),
                        reg.apos(apri)
                    )

                elif AposInfo(i = i + 1) == reg.apos(apri):
                    stall_indices[i % num_processes] = i

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

        with reg.open() as reg:

            for i in range(num_apri):
                reg.set_apos(ApriInfo(i = i), AposInfo(i = i + 2), exists_ok = True)

        # this one is forced to crash due to low time limit
        # (The reader of `cornifer.registers.Register.set_apos` will sleep for a long time)
        slurm_test_main_filename = slurm_tests_filename / 'test3d.py'
        running_max_sec = 15
        slurm_time = running_max_sec + 1
        num_processes = 7
        write_batch_file(slurm_time, slurm_test_main_filename, num_processes, f"{num_apri} {slurm_time - 10}")
        print("Submitting test batch #3d...")
        self.submit_batch()
        self.wait_till_running(allocation_max_sec, allocation_query_sec)
        print(f"Running test #3d (running_max_sec = {running_max_sec})...")
        time.sleep(slurm_time + timeout_extra_wait_sec)
        print("Checking test #3d...")
        self.check_empty_error_file()
        reg = load_shorthand("reg", test_home_dir, True)

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

                if i % num_processes == 1 and i >= 5 * num_processes + 1:
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

    def test_parallelize(self):

        slurm_test_main_filename = slurm_tests_filename / 'test4.py'
        num_apri = 1000
        num_blks = 100
        blk_len = 1000
        update_period = 10
        update_timeout = 60
        timeout = 600

        for num_procs in (1, 2, 10, 50):

            write_batch_file(timeout, slurm_test_main_filename, num_procs, f'{num_apri} {num_blks} {blk_len} {update_period} {update_timeout} {timeout}')
            print(f'Submitting test batch #4 (num_procs = {num_procs})...')
            self.submit_batch()
            self.wait_till_running(allocation_max_sec, allocation_query_sec)
            print(f'Running test #4...')
            self.wait_till_not_running(timeout, running_query_sec)
            print('Checking test #4...')
            self.check_empty_error_file()
            reg = load_shorthand('reg', test_home_dir, True)
            self.assertEqual(
                reg._write_db_filepath,
                reg._perm_db_filepath
            )

            with reg.open(readonly = True) as reg:

                for i in range(num_apri):

                    apri = ApriInfo(i = i)
                    self.assertEqual(
                        reg.apos(apri),
                        AposInfo(i = i + 1)
                    )

                    for j, blk in enumerate(reg.blks(apri)):
                        self.assertEqual(
                            blk,
                            Block(np.arange(j * blk_len, (j + 1) * blk_len), apri)
                        )