import shutil
import unittest
import subprocess
from pathlib import Path
from time import sleep

from cornifer import NumpyRegister, ApriInfo

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

        test_filename = saves_dir / 'test1.sbatch'
        error_filename = saves_dir / 'test_slurm_error.txt'
        slurm_test_main_filename = Path(__file__).parent / 'slurm_test_main.py'
        reg = NumpyRegister(saves_dir, "reg", "msg")
        blk_size = 100
        total_indices = 10050
        wait_min = 1
        apri = ApriInfo(hi = "hello")

        with reg.open(): pass

        with test_filename.open("w") as fh:
            fh.write(
f"""#!/usr/bin/env bash

#SBATCH --job-name=corniferslurmtests
#SBATCH --time=00:{wait_min:02d}:00
#SBATCH --ntasks=1
#SBATCH --ntasks-per-core=1
#SBATCH --error={error_filename}
#SBATCH --array=1-10

srun {python_command} {slurm_test_main_filename} {saves_dir} {blk_size} {total_indices} $SLURM_ARRAY_TASK_MAX $SLURM_ARRAY_TASK_ID
""")

        subprocess.run(["sbatch", str(test_filename)])
        sleep((wait_min + 1) * 60)
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


