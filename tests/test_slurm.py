import datetime
import shutil
import unittest
import subprocess
from pathlib import Path
from time import sleep

from cornifer import NumpyRegister, ApriInfo, AposInfo

saves_dir = Path.home() / "cornifer_slurm_testcases"
python_command = "sage -python"


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
        allocation_wait_sec = 30
        test_filename = saves_dir / 'test.sbatch'
        reg = NumpyRegister(saves_dir, "reg", "msg", 2 ** 40)

        slurm_test_main_filename = Path(__file__).parent / 'slurm_test_main1.py'
        blk_size = 100
        total_indices = 10050
        wait_sec = 45
        apri = ApriInfo(hi = "hello")
        slurm_array_task_max = 10

        with reg.open(): pass

        with test_filename.open("w") as fh:
            fh.write(
                sbatch_header.format(datetime.timedelta(seconds = wait_sec), slurm_array_task_max) +
                f"srun {python_command} {slurm_test_main_filename} {saves_dir} {blk_size} {total_indices} "
                f"$SLURM_ARRAY_TASK_MAX $SLURM_ARRAY_TASK_ID"
            )

        subprocess.run(["sbatch", str(test_filename)])
        subprocess.run(['squeue', '--me'])
        print(f"waiting for {allocation_wait_sec + wait_sec} seconds....")
        sleep(allocation_wait_sec + wait_sec)
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
        num_apri = 100000
        slurm_array_task_max = 2
        wait_sec = 240

        with test_filename.open("w") as fh:
            fh.write(
                sbatch_header.format(datetime.timedelta(seconds = wait_sec), slurm_array_task_max) +
                f"srun {python_command} {slurm_test_main_filename} {saves_dir} {num_apri} "
                f"$SLURM_ARRAY_TASK_MAX $SLURM_ARRAY_TASK_ID"
            )

        subprocess.run(["sbatch", str(test_filename)])
        print(f"waiting for {wait_sec + allocation_wait_sec} seconds....")
        sleep(wait_sec + allocation_wait_sec)
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

