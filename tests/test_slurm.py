from datetime import datetime, timedelta
import os
import re
import shutil
import unittest
import subprocess
from pathlib import Path
import time

import lmdb
import numpy as np
from testslurm import TestSlurm, SlurmStates

import cornifer.debug
from cornifer import NumpyRegister, ApriInfo, AposInfo, load, Block, DataNotFoundError, _utilities
from cornifer._utilities import random_unique_filename
from cornifer._utilities.lmdb import open_lmdb, r_txn_prefix_iter

error_file = 'error.txt'
sbatch_file = 'test.sbatch'
slurm_tests_filename = Path(__file__).parent / "slurm_tests"

class TestCorniferSlurm(TestSlurm, test_dir = Path.home() / 'cornifer_slurm_testcases'):

    def test_make_sigterm_raise_ReceivedSigterm(self):

        slurm_test_main_filename = slurm_tests_filename / 'test_make_sigterm_raise_ReceivedSigterm.py'
        num_processes = 1
        slurm_time = 70
        test_dir = type(self).test_dir
        self.write_batch(
            test_dir / sbatch_file,
            f'sage -python {slurm_test_main_filename} ',
            'CorniferSlurmTests', 1, num_processes, slurm_time, test_dir / error_file, None, [('--signal', 'B:TERM@5')], True
        )
        self.submit_batch()
        self.wait_till_not_state(SlurmStates.PENDING, verbose = True)
        self.wait_till_not_state(SlurmStates.RUNNING, max_sec = slurm_time + 60, verbose = True)
        self.check_error_file()

    def test_slurm_1(self):

        test_dir = type(self).test_dir
        slurm_test_main_filename = slurm_tests_filename / 'test1.py'
        num_entries = 10000
        running_max_sec = 100
        slurm_time = running_max_sec + 1
        num_processes = 10
        db_filename = "lmdb"
        self.write_batch(
            test_dir / sbatch_file,
            f'sage -python {slurm_test_main_filename} {num_processes} {test_dir} {db_filename} {num_entries}',
            'CorniferSlurmTests', 1, num_processes, slurm_time, test_dir / error_file, None, verbose = True
        )
        self.submit_batch()
        self.wait_till_not_state(SlurmStates.PENDING, verbose = True)
        self.wait_till_not_state(SlurmStates.RUNNING, max_sec = running_max_sec, verbose = True)
        self.check_error_file()
        self.assertTrue((test_dir / db_filename).exists())
        db = lmdb.open(str(test_dir / db_filename))

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
            shutil.rmtree(test_dir / db_filename)

    def test_slurm_2(self):

        test_dir = type(self).test_dir
        slurm_test_main_filename = slurm_tests_filename / 'test2.py'
        running_max_sec = 100
        slurm_time = running_max_sec + 1
        num_processes = 7
        db_filename = "lmdb"

        for num_entries in [1, 5, 10, 50, 100, 500, 1000]:

            self.write_batch(
                test_dir / sbatch_file,
                f'sage -python {slurm_test_main_filename} {num_processes} {test_dir} {db_filename} {num_entries}',
                'CorniferSlurmTests', 1, num_processes, slurm_time, test_dir / error_file, None, verbose = True
            )
            self.submit_batch()
            self.wait_till_not_state(SlurmStates.PENDING, verbose = True)
            self.wait_till_not_state(SlurmStates.RUNNING, max_sec = running_max_sec, verbose = True)
            self.check_error_file()
            start = time.time()
            max_num_queries = 100

            for _ in range(max_num_queries):

                try:
                    self.assertTrue((test_dir / db_filename).exists())

                except AssertionError:
                    time.sleep(0.5)

                else:
                    break

            else:
                raise AssertionError

            print(time.time() - start)
            db = lmdb.open(str(test_dir / db_filename))

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
                shutil.rmtree(test_dir / db_filename)

    def test_slurm_3(self):

        test_dir = type(self).test_dir
        slurm_test_main_filename = slurm_tests_filename / 'test3a.py'
        running_max_sec = 40
        blk_size = 100
        slurm_time = running_max_sec + 1
        apri = ApriInfo(hi = "hello")
        num_processes = 10
        total_indices = 10050
        reg = NumpyRegister(test_dir, "reg", "hi")
        self.write_batch(
            test_dir / sbatch_file,
            f'sage -python {slurm_test_main_filename} {num_processes} {test_dir} {blk_size} {total_indices} {slurm_time - 10}',
            'CorniferSlurmTests', 1, num_processes, slurm_time, test_dir / error_file, None, verbose = True
        )
        self.submit_batch()
        self.wait_till_not_state(SlurmStates.PENDING, verbose = True)
        self.wait_till_not_state(SlurmStates.RUNNING, max_sec = running_max_sec, verbose = True)
        self.check_error_file()
        reg = load("reg", test_dir, True)

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
        self.write_batch(
            test_dir / sbatch_file,
            f'sage -python {slurm_test_main_filename} {num_processes} {test_dir} {num_apri} {slurm_time - 10}',
            'CorniferSlurmTests', 1, num_processes, slurm_time, test_dir / error_file, None, verbose = True
        )
        self.submit_batch()
        self.wait_till_not_state(SlurmStates.PENDING, verbose = True)
        self.wait_till_not_state(SlurmStates.RUNNING, max_sec = running_max_sec, verbose = True)
        self.check_error_file()
        reg = load("reg", test_dir, True)

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

        self.write_batch(
            test_dir / sbatch_file,
            f'sage -python {slurm_test_main_filename} {num_processes} {test_dir} {num_apri} {slurm_time - 10}',
            'CorniferSlurmTests', 1, num_processes, slurm_time, test_dir / error_file, None, verbose = True
        )
        self.submit_batch()
        self.wait_till_not_state(SlurmStates.PENDING, verbose = True)
        self.wait_till_not_state(SlurmStates.RUNNING, max_sec = running_max_sec, verbose = True)
        self.check_error_file()
        time.sleep(slurm_time + 30)
        stall_indices = [None] * num_processes
        stall_indices[1] = 2 * num_processes + 1
        reg = load("reg", test_dir, True)

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
        self.write_batch(
            test_dir / sbatch_file,
            f'sage -python {slurm_test_main_filename} {num_processes} {test_dir} {num_apri} {slurm_time - 10}',
            'CorniferSlurmTests', 1, num_processes, slurm_time, test_dir / error_file, None, verbose = True
        )
        self.submit_batch()
        self.wait_till_not_state(SlurmStates.PENDING, verbose = True)
        self.wait_till_not_state(SlurmStates.RUNNING, max_sec = running_max_sec, verbose = True)
        self.check_error_file()
        reg = load("reg", test_dir, True)

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

    def test_slurm_4(self):

        test_dir = type(self).test_dir
        slurm_test_main_filename = slurm_tests_filename / 'test4.py'
        num_apri = 100
        num_blks = 100
        blk_len = 100
        timeout = 1800

        for num_procs in (10, 20):

            with (Path.home() / "parallelize.txt").open("w") as fh:
                fh.write("")

            self.write_batch(
                test_dir / sbatch_file,
                f'sage -python {slurm_test_main_filename} {num_procs} {test_dir} {num_apri} {num_blks} {blk_len}',
                'CorniferSlurmTests', 1, num_procs, timeout, test_dir / error_file, None, verbose = True
            )
            self.submit_batch()
            self.wait_till_not_state(SlurmStates.PENDING, verbose = True)
            self.wait_till_not_state(SlurmStates.RUNNING, max_sec = timeout, verbose = True)
            self.check_error_file()
            reg = load('sh', test_dir, True)

            with reg.open(readonly = True) as reg:

                for i in range(num_apri):

                    apri = ApriInfo(i = i)
                    self.assertEqual(
                        reg.apos(apri),
                        AposInfo(i = i + 1)
                    )

                    for j, blk in enumerate(reg.blks(apri)):

                        with Block(np.arange(j * blk_len, (j + 1) * blk_len), apri, j * blk_len) as blk_:
                            self.assertEqual(
                                blk,
                                blk_
                            )

            shutil.rmtree(reg._local_dir)

    def test_parallelize(self):

        test_dir = type(self).test_dir
        slurm_test_main_filename = slurm_tests_filename / 'test5.py'
        num_apri = 100
        update_period = 10
        update_timeout = 10
        blk_len = 100

        for num_procs in (20,):

            for num_blks, timeout in ((1, 60), (10, 180), (100, 600), (300, 1800), ): #(1, 60), (10, 180), (100, 600),

                for max_readers in (100, 200, 1000, 10000): # 200, 1000, 10000

                    self.write_batch(
                        test_dir / sbatch_file,
                        f'sage -python {slurm_test_main_filename} {num_procs} {test_dir} {num_apri} {num_blks} {blk_len} {update_period} {update_timeout} {timeout} {max_readers}',
                        'CorniferSlurmTests', 1, num_procs, timeout, test_dir / error_file, None, verbose = True
                    )
                    self.submit_batch(verbose = True)
                    self.wait_till_not_state(SlurmStates.PENDING, verbose = True)
                    self.wait_till_not_state(SlurmStates.RUNNING, max_sec = timeout, verbose = True)
                    self.check_error_file()
                    reg = load('sh', test_dir, True)

                    with reg.open(readonly = True) as reg:

                            for i in range(num_apri):

                                apri = ApriInfo(i = i)

                                try:
                                    self.assertEqual(
                                        reg.apos(apri),
                                        AposInfo(i = i + 1)
                                    )

                                except DataNotFoundError:

                                    print(reg.summary())
                                    raise

                                for j, blk in enumerate(reg.blks(apri)):

                                    with Block(np.arange(j * blk_len, (j + 1) * blk_len), apri, j * blk_len) as blk_:

                                        self.assertEqual(
                                            blk,
                                            blk_
                                        )

                    shutil.rmtree(reg._local_dir)