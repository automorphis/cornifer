import shutil
from pathlib import Path
from unittest import TestCase

from cornifer._utilities import random_unique_filename
from cornifer.regfilestructure import check_reg_structure, REG_FILENAME, VERSION_FILEPATH, \
    MSG_FILEPATH, CLS_FILEPATH, DATABASE_FILEPATH, MAP_SIZE_FILEPATH, TMP_DIR_FILEPATH

SAVES_DIR = random_unique_filename(Path.home())

class Test_Register_File_Structure(TestCase):

    def setUp(self):
        if SAVES_DIR.is_dir():
            shutil.rmtree(SAVES_DIR)
        SAVES_DIR.mkdir(exist_ok=False)

    def tearDown(self):
        if SAVES_DIR.is_dir():
            shutil.rmtree(SAVES_DIR)

    def test_check_register_structure(self):

        # tests absolute filepath
        with self.assertRaisesRegex(ValueError, "absolute"):
            check_reg_structure(Path("sup"))

        try:
            check_reg_structure(SAVES_DIR)

        except ValueError as e:
            if "absolute" in str(e):
                self.fail()

        except FileNotFoundError:
            pass

        local_dir = SAVES_DIR / "local_dir"
        local_dir.mkdir(exist_ok = False)

        register_filepath = local_dir / REG_FILENAME
        register_filepath.mkdir(exist_ok = False)

        with self.assertRaises(FileNotFoundError) as cm:
            check_reg_structure(local_dir)

        e = str(cm.exception)

        for filepath in [VERSION_FILEPATH, MSG_FILEPATH, CLS_FILEPATH, DATABASE_FILEPATH, MAP_SIZE_FILEPATH]:

            filepath = str(local_dir / filepath)
            self.assertIn(filepath, e)

        for filepath in [local_dir, register_filepath]:
            filepath = str(filepath)
            self.assertNotIn(filepath + ",", e)
            self.assertNotEqual(filepath, e[-len(filepath):])

        (local_dir / VERSION_FILEPATH).touch(exist_ok = False)
        (local_dir / MSG_FILEPATH).touch(exist_ok = False)
        (local_dir / CLS_FILEPATH).touch(exist_ok = False)
        (local_dir / DATABASE_FILEPATH).mkdir(exist_ok = False)
        (local_dir / MAP_SIZE_FILEPATH).touch(exist_ok = False)
        (local_dir / TMP_DIR_FILEPATH).touch(exist_ok=False)

        try:
            check_reg_structure(local_dir)

        except Exception:
            self.fail()

