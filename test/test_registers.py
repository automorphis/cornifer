import shutil
from pathlib import Path
from unittest import TestCase

from cornifer import NumPy_Register, Register
from cornifer.errors import Register_Not_Open_Error
from cornifer.registers import _BLK_KEY_PREFIX, _KEY_SEP

"""
- LEVEL 0
    - __init__
    - add_subclass
    - _split_disk_block_key
    - _join_disk_block_metadata

- LEVEL 1
    - __str__
    - __repr__
    - _check_open_raise
    - _set_local_dir

- LEVEL 2
    - _add_instance
    - __hash__ (uncreated)
    - __eq__ (uncreated)

- LEVEL 3
    - open (uncreated)

- LEVEL 4
    - __hash__ (created)
    - __eq__ (created)
    - _open_created
    - _get_instance

- LEVEL 5
    - _close_created
    - _from_name

- LEVEL 6
    - open

- LEVEL 7
    - set_msg
    - set_start_n_magnitude
    - set_start_n_residue_length
    - get_all_descriptions (no recursive)
    - _recursive_open
    - _get_descr_by_id
    - _get_id_by_descr
    - _check_no_cycles

- LEVEL 8
    - add_subregister
    - remove_subregister
"""

SAVES_DIR = Path("D:/tmp/register_tests")


class Testy_Register(Register):
    @classmethod
    def dump_disk_data(cls, data, filename): pass

    @classmethod
    def load_disk_data(cls, filename): pass

class Test_NumPy_Register(TestCase):

    def setUp(self):
        if SAVES_DIR.is_dir():
            shutil.rmtree(SAVES_DIR)
        SAVES_DIR.mkdir()

    def tearDown(self):
        if SAVES_DIR.is_dir():
            shutil.rmtree(SAVES_DIR)

    def test___init__(self):

        shutil.rmtree(SAVES_DIR)

        with self.assertRaises(FileNotFoundError):
            NumPy_Register(SAVES_DIR, "test")

        SAVES_DIR.mkdir()

        with self.assertRaises(TypeError):
            NumPy_Register(SAVES_DIR, 0)

    def test_add_subclass(self):

        with self.assertRaisesRegex(TypeError, "must be a class"):
            Register.add_subclass(0)

        class Hello:pass

        with self.assertRaisesRegex(TypeError, "subclass of `Register`"):
            Register.add_subclass(Hello)

        Register.add_subclass(Testy_Register)

        self.assertIn(
            "Testy_Register",
            Register._constructors.keys()
        )

        self.assertEqual(
            Register._constructors["Testy_Register"],
            Testy_Register
        )

    def test__split_disk_block_key(self):

        keys = [
            _BLK_KEY_PREFIX + b"{\"hello\" = \"hey\"}" + _KEY_SEP + b"00000" + _KEY_SEP + b"10",
            _BLK_KEY_PREFIX +                            _KEY_SEP + b"00000" + _KEY_SEP + b"10",
            _BLK_KEY_PREFIX + b"{\"hello\" = \"hey\"}" + _KEY_SEP +            _KEY_SEP + b"10",
            _BLK_KEY_PREFIX + b"{\"hello\" = \"hey\"}" + _KEY_SEP + b"00000" + _KEY_SEP        ,
        ]
        splits = [
            (b"{\"hello\" = \"hey\"}", b"00000", b"10"),
            (b"",                      b"00000", b"10"),
            (b"{\"hello\" = \"hey\"}", b"",      b"10"),
            (b"{\"hello\" = \"hey\"}", b"00000", b""  ),
        ]
        for key, split in zip(keys, splits):
            self.assertEqual(
                split,
                Register._split_disk_block_key(key)
            )
        for key in keys:
            self.assertEqual(
                key,
                Register._join_disk_block_metadata(*Register._split_disk_block_key(key))
            )

    def test__join_disk_block_metadata(self):

        splits = [
            (b"hello", b"there", b"friend"),
            (b"",      b"there", b"friend"),
            (b"hello", b"",      b"friend"),
            (b"hello", b"there", b""      ),
        ]
        keys = [
            _BLK_KEY_PREFIX + b"hello" + _KEY_SEP + b"there" + _KEY_SEP + b"friend",
            _BLK_KEY_PREFIX +            _KEY_SEP + b"there" + _KEY_SEP + b"friend",
            _BLK_KEY_PREFIX + b"hello" + _KEY_SEP +            _KEY_SEP + b"friend",
            _BLK_KEY_PREFIX + b"hello" + _KEY_SEP + b"there" + _KEY_SEP
        ]
        for split,key in zip(splits, keys):
            self.assertEqual(
               key,
               Register._join_disk_block_metadata(*split)
            )
        for split in splits:
            self.assertEqual(
                split,
                Register._split_disk_block_key(Register._join_disk_block_metadata(*split))
            )

    def test___str__(self):

        self.assertEqual(
            str(NumPy_Register(SAVES_DIR, "hello")),
            "hello"
        )

    def test___repr__(self):

        self.assertEqual(
            repr(NumPy_Register(SAVES_DIR, "hello")),
            f"NumPy_Register({str(SAVES_DIR)}, \"hello\")"
        )

        self.assertEqual(
            repr(Testy_Register(SAVES_DIR, "hello")),
            f"Testy_Register({str(SAVES_DIR)}, \"hello\")"
        )

    def test__check_open_raise(self):
        reg = NumPy_Register(SAVES_DIR, "hey")
        with self.assertRaisesRegex(Register_Not_Open_Error, "test\(\)"):
            reg._check_open_raise("test")

    def test__set_local_dir(self):

        local_dir = SAVES_DIR / "bad" / "test_local_dir"
        reg = NumPy_Register(SAVES_DIR, "sup")
        with self.assertRaisesRegex(ValueError, "sub-directory"):
            reg._set_local_dir(local_dir)

        local_dir = SAVES_DIR / "test_local_dir"
        reg = NumPy_Register(SAVES_DIR, "sup")
        with self.assertRaises(FileNotFoundError):
            reg._set_local_dir(local_dir)

        local_dir = SAVES_DIR / "test_local_dir"
        reg = NumPy_Register(SAVES_DIR, "sup")
        local_dir.mkdir()
        reg._set_local_dir(local_dir)
        self.assertTrue(reg._created)
        self.assertEqual(
            local_dir,
            reg._local_dir
        )

    def test__add_instance(self):
        reg = NumPy_Register(SAVES_DIR, "hey")
        Register._add_instance(reg)





