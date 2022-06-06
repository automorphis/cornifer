import shutil
from itertools import product, chain
from pathlib import Path
from unittest import TestCase

import numpy as np
import plyvel

from cornifer import Numpy_Register, Register, Apri_Info, Block
from cornifer.errors import Register_Not_Open_Error, Register_Not_Created_Error, Register_Already_Open_Error, \
    Data_Not_Found_Error, Register_Error, Subregister_Cycle_Error, Apri_Info_Not_Found_Error
from cornifer.registers import _BLK_KEY_PREFIX, _KEY_SEP, _CLS_KEY, _MSG_KEY, _CURR_ID_KEY, \
    _APRI_ID_KEY_PREFIX, _ID_APRI_KEY_PREFIX, _START_N_HEAD_KEY, _START_N_TAIL_LENGTH_KEY, _SUB_KEY_PREFIX, \
    _REGISTER_LEVELDB_NAME
from cornifer.utilities import leveldb_count_keys, leveldb_prefix_iterator

"""
- LEVEL 0
    - __init__
    - add_subclass
    - _split_disk_block_key
    - _join_disk_block_metadata

- LEVEL 1
    - __str__
    - __repr__
    - _check_open_raise (uncreated)
    - _set_local_dir
    - __hash__ (uncreated)
    - __eq__ (uncreated)
    - add_ram_block

- LEVEL 2
    - open (uncreated)
    - remove_ram_block
    - get_ram_block_by_n (no recursive)
    - get_all_ram_blocks (no recursive)
    - _iter_ram_block_metadatas 

- LEVEL 3
    - __hash__ (created)
    - __eq__ (created)
    - _check_open_raise (created)
    - _get_id_by_apri (new apri)
    
- LEVEL 4
    - _get_instance
    - set_message
    - add_disk_block
    - _get_apri_by_id
    - get_all_apri_info (no recursive)
    
- LEVEL 5
    - _from_name (same register)
    - _open_created
    - _get_id_by_apri
    - _convert_disk_block_key (no head)
    - set_start_n_info

- LEVEL 6
    - _iter_disk_block_metadatas
    - _from_name (different registers)
    - open

- LEVEL 7
    - _recursive_open
    - get_disk_block_by_metadata (no recursive)
    - remove_disk_block
    - get_all_disk_blocks

- LEVEL 8
    - _iter_subregisters
    - get_disk_block_by_n
    
- LEVEL 9
    - _check_no_cycles
    - add_subregister
    
- LEVEL 10
    - remove_subregister
    
"""

SAVES_DIR = Path("D:/tmp/tests")
# SAVES_DIR = Path.home() / "tmp" / "tests"

class Testy_Register(Register):
    @classmethod
    def dump_disk_data(cls, data, filename):
        filename.touch()
        return filename

    @classmethod
    def load_disk_data(cls, filename):
        return None

Register.add_subclass(Testy_Register)

class Testy_Register2(Register):
    @classmethod
    def dump_disk_data(cls, data, filename): pass

    @classmethod
    def load_disk_data(cls, filename): pass

class Test_Register(TestCase):

    def setUp(self):
        if SAVES_DIR.is_dir():
            shutil.rmtree(SAVES_DIR)
        SAVES_DIR.mkdir()

    def tearDown(self):
        if SAVES_DIR.is_dir():
            shutil.rmtree(SAVES_DIR)
        Register._instances.clear()

    def test___init__(self):

        shutil.rmtree(SAVES_DIR)

        with self.assertRaises(FileNotFoundError):
            Testy_Register(SAVES_DIR, "test")

        SAVES_DIR.mkdir()

        with self.assertRaises(TypeError):
            Testy_Register(SAVES_DIR, 0)

    def test_add_subclass(self):

        with self.assertRaisesRegex(TypeError, "must be a class"):
            Register.add_subclass(0)

        class Hello:pass

        with self.assertRaisesRegex(TypeError, "subclass of `Register`"):
            Register.add_subclass(Hello)

        Register.add_subclass(Testy_Register2)

        self.assertIn(
            "Testy_Register2",
            Register._constructors.keys()
        )

        self.assertEqual(
            Register._constructors["Testy_Register2"],
            Testy_Register2
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
            str(Testy_Register(SAVES_DIR, "hello")),
            "hello"
        )

    def test___repr__(self):

        self.assertEqual(
            repr(Testy_Register(SAVES_DIR, "hello")),
            f"Testy_Register(\"{str(SAVES_DIR)}\", \"hello\")"
        )

        self.assertEqual(
            repr(Testy_Register(SAVES_DIR, "hello")),
            f"Testy_Register(\"{str(SAVES_DIR)}\", \"hello\")"
        )

    def test__check_open_raise_uncreated(self):
        reg = Testy_Register(SAVES_DIR, "hey")
        with self.assertRaisesRegex(Register_Not_Open_Error, "test\(\)"):
            reg._check_open_raise("test")

    def test__set_local_dir(self):

        # test that error is raised when `local_dir` is not a sub-dir of `saves_directory`
        local_dir = SAVES_DIR / "bad" / "test_local_dir"
        reg = Testy_Register(SAVES_DIR, "sup")
        with self.assertRaisesRegex(ValueError, "sub-directory"):
            reg._set_local_dir(local_dir)

        # test that error is raised when `Register` has not been created
        local_dir = SAVES_DIR / "test_local_dir"
        reg = Testy_Register(SAVES_DIR, "sup")
        with self.assertRaisesRegex(FileNotFoundError, "database"):
            reg._set_local_dir(local_dir)

        # test that newly created register has the correct `_created` and `_local_dir` attributes
        # register database must be manually created for this test case
        local_dir = SAVES_DIR / "test_local_dir"
        reg = Testy_Register(SAVES_DIR, "sup")
        local_dir.mkdir()
        reg._reg_file = local_dir / _REGISTER_LEVELDB_NAME
        reg._db = plyvel.DB(str(reg._reg_file), create_if_missing=True)
        reg._set_local_dir(local_dir)
        self.assertTrue(reg._created)
        self.assertEqual(
            local_dir,
            reg._local_dir
        )

    def test___hash___uncreated(self):
        with self.assertRaisesRegex(Register_Not_Created_Error, "__hash__"):
            hash(Testy_Register(SAVES_DIR, "hey"))

    def test___eq___uncreated(self):
        with self.assertRaises(Register_Not_Created_Error):
            Testy_Register(SAVES_DIR, "hey") == Testy_Register(SAVES_DIR, "sup")

    def test_add_ram_block(self):

        reg = Testy_Register(SAVES_DIR, "msg")
        blk = Block([], Apri_Info(name = "test"))
        try:
            reg.add_ram_block(blk)
        except Register_Not_Open_Error:
            self.fail("register doesn't need to be open")

        reg = Testy_Register(SAVES_DIR, "msg")
        blk1 = Block([], Apri_Info(name = "test"))
        reg.add_ram_block(blk1)
        self.assertEqual(
            1,
            len(reg._ram_blks)
        )

        blk2 = Block([], Apri_Info(name = "testy"))
        reg.add_ram_block(blk2)
        self.assertEqual(
            2,
            len(reg._ram_blks)
        )

        blk3 = Block([], Apri_Info(name = "testy"))
        reg.add_ram_block(blk3)
        self.assertEqual(
            3,
            len(reg._ram_blks)
        )

        blk4 = Block([1], Apri_Info(name = "testy"))
        reg.add_ram_block(blk4)
        self.assertEqual(
            4,
            len(reg._ram_blks)
        )

    def test_open_uncreated(self):
        reg = Testy_Register(SAVES_DIR, "hey")
        with reg.open() as reg:pass
        self.assertTrue(reg._db.closed)
        self.assertTrue(reg._created)
        self.assertTrue(reg._reg_file.is_dir())
        self.assertTrue(reg._db.closed)
        keyvals = {
            _CLS_KEY : b"Testy_Register",
            _MSG_KEY : b"hey",
            _START_N_HEAD_KEY : b"0",
            _START_N_TAIL_LENGTH_KEY : str(Register._START_N_TAIL_LENGTH_DEFAULT).encode("ASCII"),
            _CURR_ID_KEY: b"0"
        }
        db = plyvel.DB(str(reg._reg_file))
        for key, val in keyvals.items():
            self.assertEqual(
                val,
                db.get(key)
            )
        self.assertEqual(
            len(keyvals),
            leveldb_count_keys(db, b"")
        )

    def test_remove_ram_block(self):

        reg = Numpy_Register(SAVES_DIR, "msg")
        blk = Block([], Apri_Info(name = "name"))
        reg.add_ram_block(blk)
        try:
            reg.remove_ram_block(blk)
        except Register_Not_Open_Error:
            self.fail("removing ram blocks doesn't need reg to be open")

        reg = Numpy_Register(SAVES_DIR, "msg")
        blk1 = Block([], Apri_Info(name = "name1"))
        reg.add_ram_block(blk1)
        reg.remove_ram_block(blk1)
        self.assertEqual(
            0,
            len(reg._ram_blks)
        )

        reg.add_ram_block(blk1)
        reg.remove_ram_block(blk1)
        self.assertEqual(
            0,
            len(reg._ram_blks)
        )

        reg.add_ram_block(blk1)
        blk2 = Block([], Apri_Info(name = "name2"))
        reg.add_ram_block(blk2)
        reg.remove_ram_block(blk1)
        self.assertEqual(
            1,
            len(reg._ram_blks)
        )

        reg.remove_ram_block(blk2)
        self.assertEqual(
            0,
            len(reg._ram_blks)
        )

    def test_get_ram_block_by_n_no_recursive(self):

        reg = Testy_Register(SAVES_DIR, "hello")
        with self.assertRaisesRegex(IndexError, "non-negative"):
            reg.get_ram_block_by_n(Apri_Info(name = "no"), -1)

        reg = Testy_Register(SAVES_DIR, "hello")
        apri = Apri_Info(name = "list")
        blk = Block(list(range(1000)), apri)
        reg.add_ram_block(blk)
        try:
            reg.get_ram_block_by_n(apri, 500)
        except Register_Not_Open_Error:
            self.fail("register does not need to be open")

        reg = Testy_Register(SAVES_DIR, "hello")
        apri = Apri_Info(name = "list")
        blk1 = Block(list(range(1000)), apri)
        reg.add_ram_block(blk1)
        for n in [0, 10, 500, 990, 999]:
            self.assertIs(
                blk1,
                reg.get_ram_block_by_n(apri, n)
            )
        for n in [1000]:
            with self.assertRaises(Data_Not_Found_Error):
                reg.get_ram_block_by_n(apri, n)

        blk2 = Block(list(range(1000, 2000)), apri, 1000)
        reg.add_ram_block(blk2)
        for n in [1000, 1010, 1990, 1999]:
            self.assertIs(
                blk2,
                reg.get_ram_block_by_n(apri, n)
            )

    def test_get_all_ram_blocks_no_recursive(self):

        reg = Testy_Register(SAVES_DIR, "msg")
        apri = Apri_Info(name = "hey")
        blk = Block([], apri)
        reg.add_ram_block(blk)
        try:
            reg.get_all_ram_blocks(apri)
        except Register_Not_Open_Error:
            self.fail("register does not need to be open")

        reg = Testy_Register(SAVES_DIR, "msg")
        apri1 = Apri_Info(name="hey")
        blk1 = Block([], apri1)
        reg.add_ram_block(blk1)
        self.assertEqual(
            1,
            len(list(reg.get_all_ram_blocks(apri1)))
        )
        self.assertEqual(
            blk1,
            list(reg.get_all_ram_blocks(apri1))[0]
        )

        apri2 = Apri_Info(name = "hello")
        blk2 = Block(list(range(10)), apri2)
        reg.add_ram_block(blk2)
        self.assertEqual(
            1,
            len(list(reg.get_all_ram_blocks(apri2)))
        )
        self.assertEqual(
            blk2,
            list(reg.get_all_ram_blocks(apri2))[0]
        )

        blk3 = Block(list(range(10)), apri2, 1)
        reg.add_ram_block(blk3)
        self.assertEqual(
            2,
            len(list(reg.get_all_ram_blocks(apri2)))
        )
        self.assertIn(
            blk2,
            reg.get_all_ram_blocks(apri2)
        )
        self.assertIn(
            blk3,
            reg.get_all_ram_blocks(apri2)
        )

    def test___hash___created(self):

        # create two `Register`s
        reg1 = Testy_Register(SAVES_DIR, "msg")
        reg2 = Testy_Register(SAVES_DIR, "msg")
        with reg1.open() as reg1:pass
        with reg2.open() as reg2:pass

        self.assertEqual(
            hash(reg1),
            hash(reg1)
        )

        self.assertEqual(
            hash(reg2),
            hash(reg2)
        )

        self.assertNotEqual(
            hash(reg1),
            hash(reg2)
        )

        # manually change the `_local_dir` to force equality
        reg2 = Testy_Register(SAVES_DIR, "msg")
        reg2._set_local_dir(reg1._local_dir)
        self.assertEqual(
            hash(reg2),
            hash(reg1)
        )

        # a different `Register` derived type should change the hash value
        reg2 = Testy_Register2(SAVES_DIR, "msg")
        reg2._set_local_dir(reg1._local_dir)
        self.assertNotEqual(
            hash(reg2),
            hash(reg1)
        )

        # relative paths should work as expected
        reg2 = Testy_Register(SAVES_DIR, "msg")
        reg2._set_local_dir(".." / SAVES_DIR / reg1._local_dir)
        self.assertEqual(
            hash(reg2),
            hash(reg1)
        )

    def test___eq___created(self):

        # open two `Register`s
        reg1 = Testy_Register(SAVES_DIR, "msg")
        reg2 = Testy_Register(SAVES_DIR, "msg")
        with reg1.open() as reg1:pass
        with reg2.open() as reg2:pass

        self.assertEqual(
            reg1,
            reg1
        )

        self.assertEqual(
            reg2,
            reg2
        )

        self.assertNotEqual(
            reg1,
            reg2
        )

        # manually change the `_local_dir` to force equality
        reg2 = Testy_Register(SAVES_DIR, "msg")
        reg2._set_local_dir(reg1._local_dir)
        self.assertEqual(
            reg2,
            reg1
        )

        # test a different `Register` derived type
        reg2 = Testy_Register2(SAVES_DIR, "msg")
        reg2._set_local_dir(reg1._local_dir)
        self.assertNotEqual(
            reg2,
            reg1
        )

        # test that relative paths work as expected
        reg2 = Testy_Register(SAVES_DIR, "msg")
        reg2._set_local_dir(".." / SAVES_DIR / reg1._local_dir)
        self.assertEqual(
            reg2,
            reg1
        )

    def test__check_open_raise_created(self):

        reg = Testy_Register(SAVES_DIR, "hi")
        with self.assertRaisesRegex(Register_Not_Open_Error, "xyz"):
            reg._check_open_raise("xyz")

        reg = Testy_Register(SAVES_DIR, "hi")
        with reg.open() as reg:
            try:
                reg._check_open_raise("xyz")
            except Register_Not_Open_Error:
                self.fail("the register is open")

        reg = Testy_Register(SAVES_DIR, "hi")
        with reg.open() as reg:pass
        with self.assertRaisesRegex(Register_Not_Open_Error, "xyz"):
            reg._check_open_raise("xyz")

    def test__get_id_by_apri_new(self):

        reg = Testy_Register(SAVES_DIR, "hi")
        with self.assertRaises(ValueError):
            reg._get_id_by_apri(None, None, True)
        with self.assertRaises(ValueError):
            reg._get_id_by_apri(None, None, False)

        apri1 = Apri_Info(name = "hi")
        apri2 = Apri_Info(name = "hello")
        apri3 = Apri_Info(name = "sup")
        apri4 = Apri_Info(name = "hey")
        reg = Testy_Register(SAVES_DIR, "hi")
        with reg.open() as reg:
            curr_id = reg._db.get(_CURR_ID_KEY)
            _id1 = reg._get_id_by_apri(apri1, None, True)
            self.assertEqual(
                curr_id,
                _id1
            )
            self.assertEqual(
                1,
                leveldb_count_keys(reg._db, _APRI_ID_KEY_PREFIX)
            )
            self.assertEqual(
                1,
                leveldb_count_keys(reg._db, _ID_APRI_KEY_PREFIX)
            )

            _id2 = reg._get_id_by_apri(apri2, None, True)
            self.assertNotEqual(
                _id1,
                _id2
            )
            self.assertEqual(
                2,
                leveldb_count_keys(reg._db, _APRI_ID_KEY_PREFIX)
            )
            self.assertEqual(
                2,
                leveldb_count_keys(reg._db, _ID_APRI_KEY_PREFIX)
            )

            _id3 = reg._get_id_by_apri(None, apri3.to_json().encode("ASCII"), True)
            self.assertNotIn(
                _id3,
                [_id1, _id2]
            )
            self.assertEqual(
                3,
                leveldb_count_keys(reg._db, _APRI_ID_KEY_PREFIX)
            )
            self.assertEqual(
                3,
                leveldb_count_keys(reg._db, _ID_APRI_KEY_PREFIX)
            )

            with self.assertRaises(Apri_Info_Not_Found_Error):
                reg._get_id_by_apri(apri4, None, False)

    def test__get_instance(self):

        reg1 = Testy_Register(SAVES_DIR, "msg")
        with reg1.open() as reg1: pass
        reg2 = Testy_Register(SAVES_DIR, "msg")
        reg2._set_local_dir(reg1._local_dir)
        self.assertIs(
            reg1,
            Register._get_instance(reg2._local_dir)
        )
        self.assertIs(
            reg1,
            Register._get_instance(reg1._local_dir)
        )

    def test_set_message(self):

        reg = Testy_Register(SAVES_DIR, "testy")
        with self.assertRaisesRegex(Register_Not_Open_Error, "set_message"):
            reg.set_message("no")

        reg = Testy_Register(SAVES_DIR, "testy")
        with reg.open() as reg:
            reg.set_message("yes")
            self.assertEqual(
                b"yes",
                reg._db.get(_MSG_KEY)
            )
        self.assertEqual(
            "yes",
            str(reg)
        )
        self.assertEqual(
            b"yes",
            reg._msg_bytes
        )

    def test_add_disk_block(self):

        reg = Testy_Register(SAVES_DIR, "sup")
        blk = Block([], Apri_Info(name = "hi"))
        with self.assertRaisesRegex(Register_Not_Open_Error, "add_disk_block"):
            reg.add_disk_block(blk)

        reg = Testy_Register(SAVES_DIR, "hello")
        blk = Block([], Apri_Info(name = "hi"), 10**50)
        with reg.open() as reg:
            with self.assertRaisesRegex(IndexError, "correct head"):
                reg.add_disk_block(blk)

        reg = Testy_Register(SAVES_DIR, "hello")
        too_large = reg._start_n_tail_mod
        blk = Block([], Apri_Info(name = "hi"), too_large)
        with reg.open() as reg:
            with self.assertRaisesRegex(IndexError, "correct head"):
                reg.add_disk_block(blk)

        reg = Testy_Register(SAVES_DIR, "hello")
        too_large = reg._start_n_tail_mod
        blk = Block([], Apri_Info(name = "hi"), too_large - 1)
        with reg.open() as reg:
            try:
                reg.add_disk_block(blk)
            except IndexError:
                self.fail("index is not too large")

        reg = Testy_Register(SAVES_DIR, "hi")
        blk1 = Block([], Apri_Info(name = "hello"))
        blk2 = Block([1], Apri_Info(name = "hello"))
        blk3 = Block([], Apri_Info(name = "hi"))
        blk4 = Block([], Apri_Info(name = "hello"))
        with reg.open() as reg:

            reg.add_disk_block(blk1)
            self.assertEqual(
                1,
                leveldb_count_keys(reg._db, _BLK_KEY_PREFIX)
            )
            self.assertEqual(
                1,
                leveldb_count_keys(reg._db, _APRI_ID_KEY_PREFIX)
            )
            self.assertEqual(
                1,
                leveldb_count_keys(reg._db, _ID_APRI_KEY_PREFIX)
            )

            reg.add_disk_block(blk2)
            self.assertEqual(
                2,
                leveldb_count_keys(reg._db, _BLK_KEY_PREFIX)
            )
            self.assertEqual(
                1,
                leveldb_count_keys(reg._db, _APRI_ID_KEY_PREFIX)
            )
            self.assertEqual(
                1,
                leveldb_count_keys(reg._db, _ID_APRI_KEY_PREFIX)
            )

            reg.add_disk_block(blk3)
            self.assertEqual(
                3,
                leveldb_count_keys(reg._db, _BLK_KEY_PREFIX)
            )
            self.assertEqual(
                2,
                leveldb_count_keys(reg._db, _APRI_ID_KEY_PREFIX)
            )
            self.assertEqual(
                2,
                leveldb_count_keys(reg._db, _ID_APRI_KEY_PREFIX)
            )

            reg.add_disk_block(blk4)
            self.assertEqual(
                3,
                leveldb_count_keys(reg._db, _BLK_KEY_PREFIX)
            )
            self.assertEqual(
                2,
                leveldb_count_keys(reg._db, _APRI_ID_KEY_PREFIX)
            )
            self.assertEqual(
                2,
                leveldb_count_keys(reg._db, _ID_APRI_KEY_PREFIX)
            )

    def test__get_apri_by_id(self):

        reg = Testy_Register(SAVES_DIR, "hello")
        with reg.open() as reg:
            apri1 = Apri_Info(name = "hi")
            _id1 = reg._get_id_by_apri(apri1, None, True)

            self.assertIsInstance(
                _id1,
                bytes
            )
            self.assertEqual(
                apri1,
                Apri_Info.from_json(reg._get_apri_by_id(_id1).decode("ASCII"))
            )

            apri2 = Apri_Info(name = "sup")
            _id2 = reg._get_id_by_apri(apri2, None, True)
            self.assertEqual(
                apri2,
                Apri_Info.from_json(reg._get_apri_by_id(_id2).decode("ASCII"))
            )


    def test_get_all_apri_info_no_recursive(self):

        reg = Testy_Register(SAVES_DIR, "msg")
        with self.assertRaisesRegex(Register_Not_Open_Error, "get_all_apri_info"):
            reg.get_all_apri_info()

        reg = Testy_Register(SAVES_DIR, "msg")
        with reg.open() as reg:

            apri1 = Apri_Info(name = "hello")
            reg._get_id_by_apri(apri1, None, True)
            self.assertEqual(
                1,
                len(list(reg.get_all_apri_info()))
            )
            self.assertEqual(
                apri1,
                list(reg.get_all_apri_info())[0]
            )

            apri2 = Apri_Info(name = "hey")
            blk = Block([], apri2)
            reg.add_ram_block(blk)

            self.assertEqual(
                2,
                len(list(reg.get_all_apri_info()))
            )
            self.assertIn(
                apri1,
                list(reg.get_all_apri_info())
            )
            self.assertIn(
                apri2,
                list(reg.get_all_apri_info())
            )

    # def test__from_name_same_register(self):
    #
    #     reg = Testy_Register2(SAVES_DIR, "hello")
    #     with reg.open() as reg: pass
    #     with self.assertRaisesRegex(TypeError, "add_subclass"):
    #         Register._from_local_dir(reg._local_dir)
    #
    #     reg1 = Testy_Register(SAVES_DIR, "hellooooo")
    #     with reg1.open() as reg1: pass
    #     reg2 = Register._from_local_dir(reg1._local_dir)
    #     self.assertIs(
    #         reg1,
    #         reg2
    #     )

    def test__open_created(self):

        reg = Testy_Register(SAVES_DIR, "testy")
        with reg.open() as reg: pass
        with reg.open() as reg:
            self.assertFalse(reg._db.closed)
            with self.assertRaises(Register_Already_Open_Error):
                with reg.open() as reg: pass

        reg1 = Testy_Register(SAVES_DIR, "testy")
        with reg1.open() as reg1: pass
        reg2 = Testy_Register(SAVES_DIR, "testy")
        reg2._set_local_dir(reg1._local_dir)
        self.assertEqual(
            reg1,
            reg2
        )
        self.assertFalse(
            reg1 is reg2
        )
        with reg2.open() as reg2:
            self.assertIs(
                reg1,
                reg2
            )

    def test__get_id_by_apri(self):

        reg = Testy_Register(SAVES_DIR, "hello")
        apri1 = Apri_Info(name = "hello")
        with reg.open() as reg:
            _id1 = reg._get_id_by_apri(apri1, None, True)
            _id2 = reg._get_id_by_apri(apri1, None, True)
            self.assertIsInstance(
                _id2,
                bytes
            )
            self.assertEqual(
                _id1,
                _id2
            )

            _id3 = reg._get_id_by_apri(None, apri1.to_json().encode("ASCII"), False)
            self.assertEqual(
                _id1,
                _id3
            )

    def test__convert_disk_block_key_no_head(self):

        reg = Testy_Register(SAVES_DIR, "sup")
        with reg.open() as reg:

            apri1 = Apri_Info(name = "hey")
            blk1 = Block([], apri1)
            reg.add_disk_block(blk1)
            with leveldb_prefix_iterator(reg._db, _BLK_KEY_PREFIX) as it:
                for curr_key,_ in it: pass
            self.assertEqual(
                (apri1, 0, 0),
                reg._convert_disk_block_key(curr_key)
            )
            self.assertEqual(
                (apri1, 0, 0),
                reg._convert_disk_block_key(curr_key, apri1)
            )
            old_keys = {curr_key}

            blk2 = Block(list(range(10)), apri1)
            reg.add_disk_block(blk2)
            with leveldb_prefix_iterator(reg._db, _BLK_KEY_PREFIX) as it:
                for key,_val in it:
                    if key not in old_keys:
                        curr_key = key
            self.assertEqual(
                (apri1, 0, 10),
                reg._convert_disk_block_key(curr_key)
            )
            old_keys.add(curr_key)

            apri2 = Apri_Info(name = "hello")
            blk3 = Block(list(range(100)), apri2, 10)
            reg.add_disk_block(blk3)
            with leveldb_prefix_iterator(reg._db, _BLK_KEY_PREFIX) as it:
                for key,_val in it:
                    if key not in old_keys:
                        curr_key = key
            self.assertEqual(
                (apri2, 10, 100),
                reg._convert_disk_block_key(curr_key)
            )
            old_keys.add(curr_key)

            blk4 = Block(list(range(100)), apri2)
            reg.add_disk_block(blk4)
            with leveldb_prefix_iterator(reg._db, _BLK_KEY_PREFIX) as it:
                for key,_val in it:
                    if key not in old_keys:
                        curr_key = key
            self.assertEqual(
                (apri2, 0, 100),
                reg._convert_disk_block_key(curr_key)
            )

    def check_reg_set_start_n_info(self, reg, mod, head, tail_length):
        self.assertEqual(
            mod,
            reg._start_n_tail_mod
        )
        self.assertEqual(
            head,
            reg._start_n_head
        )
        self.assertEqual(
            tail_length,
            reg._start_n_tail_length
        )
        self.assertEqual(
            str(head).encode("ASCII"),
            reg._db.get(_START_N_HEAD_KEY)
        )
        self.assertEqual(
            str(tail_length).encode("ASCII"),
            reg._db.get(_START_N_TAIL_LENGTH_KEY)
        )

    def check_key_set_start_n_info(self, reg, key, apri, start_n, length):
        _apri, _start_n, _length = reg._convert_disk_block_key(key, None)
        self.assertEqual(
            apri,
            _apri
        )
        self.assertEqual(
            start_n,
            _start_n
        )
        self.assertEqual(
            length,
            _length
        )

    def test_set_start_n_info(self):

        reg = Testy_Register(SAVES_DIR, "hello")
        with self.assertRaisesRegex(Register_Not_Open_Error, "set_start_n_info"):
            reg.set_start_n_info(10, 3)

        reg = Testy_Register(SAVES_DIR, "hello")
        with reg.open() as reg:
            with self.assertRaisesRegex(TypeError, "int"):
                reg.set_start_n_info(10, 3.5)

        reg = Testy_Register(SAVES_DIR, "hello")
        with reg.open() as reg:
            with self.assertRaisesRegex(TypeError, "int"):
                reg.set_start_n_info(10.5, 3)

        reg = Testy_Register(SAVES_DIR, "hello")
        with reg.open() as reg:
            with self.assertRaisesRegex(ValueError, "non-negative"):
                reg.set_start_n_info(-1, 3)

        reg = Testy_Register(SAVES_DIR, "hello")
        with reg.open() as reg:
            try:
                reg.set_start_n_info(0, 3)
            except ValueError:
                self.fail("head can be 0")

        reg = Testy_Register(SAVES_DIR, "hello")
        with reg.open() as reg:
            with self.assertRaisesRegex(ValueError, "non-negative"):
                reg.set_start_n_info(0, -1)

        for head, tail_length in product([0, 1, 10, 100, 1100, 450], [1,2,3,4,5]):

            # check set works
            reg = Testy_Register(SAVES_DIR, "hello")
            with reg.open() as reg:

                try:
                    reg.set_start_n_info(head, tail_length)
                except ValueError:
                    self.fail(f"head = {head}, tail_length = {tail_length} are okay")


                self.assertEqual(
                    str(head).encode("ASCII"),
                    reg._db.get(_START_N_HEAD_KEY)
                )
                self.assertEqual(
                    str(tail_length).encode("ASCII"),
                    reg._db.get(_START_N_TAIL_LENGTH_KEY)
                )

            # test make sure ValueError is thrown for small smart_n
            # 0 and head * 10 ** tail_length - 1 are the two possible extremes of the small start_n
            if head > 0:
                for start_n in [0, head * 10 ** tail_length - 1]:
                    reg = Testy_Register(SAVES_DIR, "hello")
                    with reg.open() as reg:
                            blk = Block([], Apri_Info(name = "hi"), start_n)
                            reg.add_disk_block(blk)
                            with self.assertRaisesRegex(ValueError, "correct head"):
                                reg.set_start_n_info(head, tail_length)

                            # make sure it exits safely
                            self.check_reg_set_start_n_info(
                                reg,
                                10 ** Register._START_N_TAIL_LENGTH_DEFAULT, 0, Register._START_N_TAIL_LENGTH_DEFAULT
                            )

            # test to make sure a few permissible start_n work
            smallest = head * 10 ** tail_length
            largest = smallest + 10 ** tail_length  - 1
            for start_n in [smallest, smallest + 1, smallest + 2, largest -2, largest -1, largest]:
                reg = Testy_Register(SAVES_DIR, "hello")
                apri = Apri_Info(name="hi")
                with reg.open() as reg:
                    blk = Block([], apri,start_n)
                    reg.add_disk_block(blk)
                    try:
                        reg.set_start_n_info(head, tail_length)
                    except ValueError:
                        self.fail()

                    self.check_reg_set_start_n_info(
                        reg,
                        10 ** tail_length, head, tail_length
                    )

                    with leveldb_prefix_iterator(reg._db, _BLK_KEY_PREFIX) as it:
                        for curr_key,_ in it:pass

                    self.check_key_set_start_n_info(
                        reg, curr_key,
                        apri, start_n, 0
                    )
                    old_keys = {curr_key}

                    blk = Block(list(range(50)), apri, start_n)


            # test to make sure `largest + 1` etc do not work
            for start_n in [largest + 1, largest + 10, largest + 100, largest + 1000]:
                reg = Testy_Register(SAVES_DIR, "hello")
                apri = Apri_Info(name="hi")
                with reg.open() as reg:
                    blk = Block([], apri, start_n)
                    reg.add_disk_block(blk)
                    with self.assertRaisesRegex(ValueError, "correct head"):
                        reg.set_start_n_info(head, tail_length)

                    # make sure it exits safely
                    self.check_reg_set_start_n_info(
                        reg,
                        10 ** Register._START_N_TAIL_LENGTH_DEFAULT, 0, Register._START_N_TAIL_LENGTH_DEFAULT
                    )

    def check__iter_disk_block_metadatas(self, t, apri, start_n, length):
        self.assertEqual(
            4,
            len(t)
        )
        self.assertIsInstance(
            t[0],
            Apri_Info
        )
        self.assertEqual(
            apri,
            t[0]
        )
        self.assertIsInstance(
            t[1],
            int
        )
        self.assertEqual(
            start_n,
            t[1]
        )
        self.assertIsInstance(
            t[2],
            int
        )
        self.assertEqual(
            length,
            t[2]
        )
        self.assertIsInstance(
            t[3],
            Path
        )

    def test__iter_disk_block_metadatas(self):

        reg = Testy_Register(SAVES_DIR, "HI")
        with reg.open() as reg:
            apri1 = Apri_Info(name = "abc")
            apri2 = Apri_Info(name = "xyz")
            blk1 = Block(list(range(50)), apri1, 0)
            blk2 = Block(list(range(50)), apri1, 50)
            blk3 = Block(list(range(500)), apri2, 1000)

            reg.add_disk_block(blk1)
            total = 0
            for i, t in chain(
                enumerate(reg._iter_disk_block_metadatas(None, None)),
                enumerate(reg._iter_disk_block_metadatas(apri1, None)),
                enumerate(reg._iter_disk_block_metadatas(None, apri1.to_json().encode("ASCII")))
            ):
                total += 1
                if i == 0:
                    self.check__iter_disk_block_metadatas(t, apri1, 0, 50)
                else:
                    self.fail()
            if total != 3:
                self.fail(str(total))

            reg.add_disk_block(blk2)
            total = 0
            for i, t in chain(
                enumerate(reg._iter_disk_block_metadatas(None, None)),
                enumerate(reg._iter_disk_block_metadatas(apri1, None)),
                enumerate(reg._iter_disk_block_metadatas(None, apri1.to_json().encode("ASCII")))
            ):
                total += 1
                if i == 0:
                    self.check__iter_disk_block_metadatas(t, apri1, 0, 50)
                elif i == 1:
                    self.check__iter_disk_block_metadatas(t, apri1, 50, 50)
                else:
                    self.fail()
            if total != 6:
                self.fail(str(total))

            reg.add_disk_block(blk3)
            total = 0
            for i, t in chain(
                enumerate(reg._iter_disk_block_metadatas(None, None))
            ):
                total += 1
                if i == 0:
                    self.check__iter_disk_block_metadatas(t, apri1, 0, 50)
                elif i == 1:
                    self.check__iter_disk_block_metadatas(t, apri1, 50, 50)
                elif i == 2:
                    self.check__iter_disk_block_metadatas(t, apri2, 1000, 500)
                else:
                    self.fail()
            if total != 3:
                self.fail()

            total = 0
            for i, t in chain(
                enumerate(reg._iter_disk_block_metadatas(apri1, None)),
                enumerate(reg._iter_disk_block_metadatas(None, apri1.to_json().encode("ASCII")))
            ):
                total += 1
                if i == 0:
                    self.check__iter_disk_block_metadatas(t, apri1, 0, 50)
                elif i == 1:
                    self.check__iter_disk_block_metadatas(t, apri1, 50, 50)
                else:
                    self.fail()

            if total != 4:
                self.fail()

    # def test__from_local_dir_different_registers(self):
    #
    #     reg1 = Testy_Register(SAVES_DIR, "hellooooo")
    #     with reg1.open() as reg1: pass
    #
    #     reg2 = Testy_Register(SAVES_DIR, "hellooooo")
    #     with reg2.open() as reg2: pass
    #
    #     del Register._instances[reg2]
    #
    #     reg3 = Register._from_local_dir(reg2._local_dir)
    #
    #     self.assertEqual(
    #         reg2,
    #         reg3
    #     )
    #     self.assertFalse(
    #         reg2 is reg3
    #     )

    def test_open(self):

        reg1 = Testy_Register(SAVES_DIR, "msg")
        with reg1.open() as reg2:pass
        self.assertIs(
            reg1,
            reg2
        )
        try:
            with reg1.open() as reg1:pass
        except Register_Error:
            self.fail()

        reg2 = Testy_Register(SAVES_DIR, "hello")
        with reg2.open() as reg2:pass
        reg3 = Testy_Register(SAVES_DIR, "hello")
        reg3._set_local_dir(reg2._local_dir)
        with reg3.open() as reg4:pass
        self.assertIs(
            reg4,
            reg2
        )

    def test__recursive_open(self):

        reg = Testy_Register(SAVES_DIR, "hello")
        with self.assertRaises(Register_Not_Created_Error):
            with reg._recursive_open():pass

        reg1 = Testy_Register(SAVES_DIR, "hello")
        with reg1.open() as reg1:pass
        with reg1._recursive_open() as reg2:pass
        self.assertIs(
            reg1,
            reg2
        )

        reg2 = Testy_Register(SAVES_DIR, "hello")
        reg2._set_local_dir(reg1._local_dir)
        with reg2._recursive_open() as reg3:pass
        self.assertIs(
            reg1,
            reg3
        )

        reg = Testy_Register(SAVES_DIR, "hi")
        with reg.open():
            try:
                with reg._recursive_open():pass
            except Register_Error:
                self.fail()
            self.assertFalse(
                reg._db.closed
            )
        self.assertTrue(
            reg._db.closed
        )

    def test_get_disk_block_no_recursive(self):

        reg = Numpy_Register(SAVES_DIR, "hello")
        with self.assertRaisesRegex(Register_Not_Open_Error, "get_disk_block"):
            reg.get_disk_block(Apri_Info(name = "i am the octopus"), 0, 0)

        reg = Numpy_Register(SAVES_DIR, "hello")
        with reg.open() as reg:
            apri1 = Apri_Info(name = "i am the octopus")
            blk1 = Block(np.arange(100), apri1)
            reg.add_disk_block(blk1)
            self.assertEqual(
                blk1,
                reg.get_disk_block(apri1, 0, 100)
            )

            blk2 = Block(np.arange(100,200), apri1, 100)
            reg.add_disk_block(blk2)
            self.assertEqual(
                blk2,
                reg.get_disk_block(apri1, 100, 100)
            )
            self.assertEqual(
                blk1,
                reg.get_disk_block(apri1, 0, 100)
            )

            apri2 = Apri_Info(name = "hello")
            blk3 = Block(np.arange(3000,4000), apri2, 2000)
            reg.add_disk_block(blk3)
            self.assertEqual(
                blk3,
                reg.get_disk_block(apri2, 2000, 1000)
            )
            self.assertEqual(
                blk2,
                reg.get_disk_block(apri1, 100, 100)
            )
            self.assertEqual(
                blk1,
                reg.get_disk_block(apri1, 0, 100)
            )

            for metadata in [
                (apri1, 0, 200), (apri1, 1, 99), (apri1, 5, 100), (apri1, 1, 100),
                (apri2, 2000, 999), (apri2, 2000, 1001), (apri2, 1999, 1000),
                (Apri_Info(name = "noooo"), 0, 100)
            ]:
                with self.assertRaises(Data_Not_Found_Error):
                    reg.get_disk_block(*metadata)

    def test_remove_disk_block(self):

        reg1 = Testy_Register(SAVES_DIR, "hi")
        with self.assertRaisesRegex(Register_Not_Open_Error, "remove_disk_block"):
            reg1.remove_disk_block(Apri_Info(name = "fooopy doooopy"), 0, 0)

        reg1 = Testy_Register(SAVES_DIR, "hi")
        with reg1.open() as reg1:
            apri1 = Apri_Info(name = "fooopy doooopy")
            blk1 = Block(list(range(50)), apri1)
            reg1.add_disk_block(blk1)
            filename = Path(reg1._db.get(reg1._get_disk_block_key(apri1, None, 0, 50)).decode("ASCII"))
            self.assertTrue(
                filename.is_file()
            )
            self.assertEqual(
                1,
                leveldb_count_keys(reg1._db, _BLK_KEY_PREFIX)
            )
            reg1.remove_disk_block(apri1, 0, 50)
            self.assertFalse(
                filename.is_file()
            )
            self.assertEqual(
                0,
                leveldb_count_keys(reg1._db, _BLK_KEY_PREFIX)
            )

            reg1.add_disk_block(blk1)
            apri2 = Apri_Info(name = "fooopy doooopy2")
            blk2 = Block(list(range(100)), apri2, 1000)
            reg1.add_disk_block(blk2)
            filename = Path(reg1._db.get(reg1._get_disk_block_key(apri2, None, 1000, 100)).decode("ASCII"))
            self.assertTrue(
                filename.is_file()
            )
            self.assertEqual(
                2,
                leveldb_count_keys(reg1._db, _BLK_KEY_PREFIX)
            )
            reg1.remove_disk_block(apri2, 1000, 100)
            self.assertFalse(
                filename.is_file()
            )
            self.assertEqual(
                1,
                leveldb_count_keys(reg1._db, _BLK_KEY_PREFIX)
            )

        # add the same block to two registers
        reg1 = Testy_Register(SAVES_DIR, "hello")
        reg2 = Testy_Register(SAVES_DIR, "sup")
        apri = Apri_Info(name = "hi")
        blk = Block([], apri)
        with reg1.open() as reg1:
            reg1.add_disk_block(blk)
        with reg2.open() as reg2:
            reg2.add_disk_block(blk)
        with reg1.open() as reg1:
            reg1.remove_disk_block(apri, 0, 0)
            self.assertEqual(
                0,
                leveldb_count_keys(reg1._db, _BLK_KEY_PREFIX)
            )
        with reg2.open() as reg2:
            self.assertEqual(
                1,
                leveldb_count_keys(reg2._db, _BLK_KEY_PREFIX)
            )

    def test_get_all_disk_blocks_no_recursive(self):

        reg = Numpy_Register(SAVES_DIR, "HI")
        with reg.open() as reg:
            apri1 = Apri_Info(name = "abc")
            apri2 = Apri_Info(name = "xyz")
            blk1 = Block(np.arange(50), apri1, 0)
            blk2 = Block(np.arange(50), apri1, 50)
            blk3 = Block(np.arange(500), apri2, 1000)

            reg.add_disk_block(blk1)
            total = 0
            for i, blk in enumerate(reg.get_all_disk_blocks(apri1)):
                total += 1
                if i == 0:
                    self.assertEqual(
                        blk1,
                        blk
                    )
                else:
                    self.fail()
            self.assertEqual(
                1,
                total
            )

            reg.add_disk_block(blk2)
            total = 0
            for i, blk in enumerate(reg.get_all_disk_blocks(apri1)):
                total += 1
                if i == 0:
                    self.assertEqual(
                        blk1,
                        blk
                    )
                elif i == 1:
                    self.assertEqual(
                        blk2,
                        blk
                    )
                else:
                    self.fail()
            self.assertEqual(
                2,
                total
            )

            reg.add_disk_block(blk3)
            total = 0
            for i, blk in enumerate(reg.get_all_disk_blocks(apri1)):
                total += 1
                if i == 0:
                    self.assertEqual(
                        blk1,
                        blk
                    )
                elif i == 1:
                    self.assertEqual(
                        blk2,
                        blk
                    )
                else:
                    self.fail()
            self.assertEqual(
                2,
                total
            )
            total = 0
            for i,blk in enumerate(reg.get_all_disk_blocks(apri2)):
                total += 1
                if i == 0:
                    self.assertEqual(
                        blk3,
                        blk
                    )
                else:
                    self.fail()
            self.assertEqual(
                1,
                total
            )

    def test__iter_subregisters(self):

        reg = Testy_Register(SAVES_DIR, "hello")
        with reg.open() as reg:
            total = 0
            for i,_ in enumerate(reg._iter_subregisters()):
                total += 1
            self.assertEqual(
                0,
                total
            )


        reg = Testy_Register(SAVES_DIR, "hello")
        with reg.open() as reg:
            reg._db.put(reg._get_subregister_key(), reg._reg_cls_bytes)
            total = 0
            for i, _reg in enumerate(reg._iter_subregisters()):
                total += 1
                if i == 0:
                    self.assertIs(
                        reg,
                        _reg
                    )
                else:
                    self.fail()
            self.assertEqual(
                1,
                total
            )

        reg1 = Testy_Register(SAVES_DIR, "hello")
        reg2 = Testy_Register(SAVES_DIR, "hello")
        reg3 = Testy_Register(SAVES_DIR, "hello")
        with reg2.open():pass
        with reg3.open():pass
        with reg1.open() as reg:
            reg1._db.put(reg2._get_subregister_key(), reg._reg_cls_bytes)
            reg1._db.put(reg3._get_subregister_key(), reg._reg_cls_bytes)
            total = 0
            regs = []
            for i, _reg in enumerate(reg1._iter_subregisters()):
                total += 1
                if i == 0 or i == 1:
                    self.assertTrue(
                        _reg is reg2 or _reg is reg3
                    )
                    regs.append(_reg)
                else:
                    self.fail()
            self.assertEqual(
                2,
                total
            )
            self.assertFalse(
                regs[0] is regs[1]
            )

        reg1 = Testy_Register(SAVES_DIR, "hello")
        reg2 = Testy_Register(SAVES_DIR, "hello")
        reg3 = Testy_Register(SAVES_DIR, "hello")
        with reg3.open():pass
        with reg2.open():
            reg2._db.put(reg3._get_subregister_key(), reg._reg_cls_bytes)
        with reg1.open() as reg:
            reg1._db.put(reg2._get_subregister_key(), reg._reg_cls_bytes)
            total = 0
            regs = []
            for i, _reg in enumerate(reg._iter_subregisters()):
                total += 1
                if i == 0:
                    self.assertTrue(
                        _reg is reg2
                    )
                    regs.append(_reg)
                else:
                    self.fail()
            self.assertEqual(
                1,
                total
            )
        with reg2.open() as reg:
            total = 0
            regs = []
            for i, _reg in enumerate(reg._iter_subregisters()):
                total += 1
                if i == 0:
                    self.assertTrue(
                        _reg is reg3
                    )
                    regs.append(_reg)
                else:
                    self.fail()
            self.assertEqual(
                1,
                total
            )

    def test_get_disk_block_by_n_no_recursive(self):

        reg = Numpy_Register(SAVES_DIR, "hello")
        with self.assertRaises(Register_Not_Open_Error):
            reg.get_disk_block_by_n(Apri_Info(name = "no"), 50)

        reg = Numpy_Register(SAVES_DIR, "hello")
        apri1 = Apri_Info(name = "sup")
        apri2 = Apri_Info(name = "hi")
        blk1 = Block(np.arange(75), apri1)
        blk2 = Block(np.arange(125), apri1, 75)
        blk3 = Block(np.arange(1000), apri2, 100)
        blk4 = Block(np.arange(100), apri2, 2000)
        with reg.open() as reg:
            reg.add_disk_block(blk1)
            reg.add_disk_block(blk2)
            reg.add_disk_block(blk3)
            reg.add_disk_block(blk4)
            for n in [0, 1, 2, 72, 73, 74]:
                self.assertEqual(
                    blk1,
                    reg.get_disk_block_by_n(apri1, n)
                )
            for n in [75, 76, 77, 197, 198, 199]:
                self.assertEqual(
                    blk2,
                    reg.get_disk_block_by_n(apri1, n)
                )
            for n in [-2, -1]:
                with self.assertRaisesRegex(ValueError, "positive"):
                    reg.get_disk_block_by_n(apri1, n)
            for n in [200, 201, 1000]:
                with self.assertRaises(Data_Not_Found_Error):
                    reg.get_disk_block_by_n(apri1, n)

    def test__check_no_cycles(self):

        reg = Testy_Register(SAVES_DIR, "hello")
        with self.assertRaises(Register_Not_Created_Error):
            reg._check_no_cycles(reg)

        reg = Testy_Register(SAVES_DIR, "hello")
        with reg.open():pass
        # loop
        self.assertFalse(
            reg._check_no_cycles(reg)
        )

        reg1 = Testy_Register(SAVES_DIR, "hello")
        reg2 = Testy_Register(SAVES_DIR, "hello")
        reg3 = Testy_Register(SAVES_DIR, "hello")
        reg4 = Testy_Register(SAVES_DIR, "hello")
        with reg1.open(): pass
        with reg2.open(): pass
        with reg3.open(): pass
        with reg4.open(): pass

        # ok
        self.assertTrue(
            reg2._check_no_cycles(reg1)
        )

        with reg1.open() as reg1:
            reg1._db.put(reg2._get_subregister_key(), reg2._reg_cls_bytes)
        # 2-cycle
        self.assertFalse(
            reg1._check_no_cycles(reg2)
        )

        with reg2.open() as reg2:
            reg2._db.put(reg3._get_subregister_key(), reg3._reg_cls_bytes)
        # 2-cycle with tail
        self.assertFalse(
            reg2._check_no_cycles(reg3)
        )
        # 3-cycle
        self.assertFalse(
            reg1._check_no_cycles(reg3)
        )
        # 2-path with shortcut
        self.assertTrue(
            reg3._check_no_cycles(reg1)
        )

        with reg3.open() as reg3:
            reg3._db.put(reg4._get_subregister_key(), reg4._reg_cls_bytes)

        # 4-cycle
        self.assertFalse(
            reg1._check_no_cycles(reg4)
        )

        # 3-path with shortcut
        self.assertTrue(
            reg4._check_no_cycles(reg1)
        )

    def test_add_subregister(self):

        reg1 = Testy_Register(SAVES_DIR, "hello")
        reg2 = Testy_Register(SAVES_DIR, "hello")
        with self.assertRaisesRegex(Register_Not_Open_Error, "add_subregister"):
            reg1.add_subregister(reg2)

        reg1 = Testy_Register(SAVES_DIR, "hello")
        reg2 = Testy_Register(SAVES_DIR, "hello")
        with reg1.open() as reg1:
            with self.assertRaisesRegex(Register_Not_Created_Error, "add_subregister"):
                reg1.add_subregister(reg2)

        reg1 = Testy_Register(SAVES_DIR, "hello")
        reg2 = Testy_Register(SAVES_DIR, "hello")
        reg3 = Testy_Register(SAVES_DIR, "hello")
        with reg2.open(): pass
        with reg1.open() as reg1:
            try:
                reg1.add_subregister(reg2)
            except Subregister_Cycle_Error:
                self.fail()

        with reg3.open(): pass
        with reg2.open() as reg2:
            try:
                reg2.add_subregister(reg3)
            except Subregister_Cycle_Error:
                self.fail()
        with reg1.open() as reg1:
            try:
                reg1.add_subregister(reg3)
            except Subregister_Cycle_Error:
                self.fail()

        reg1 = Testy_Register(SAVES_DIR, "hello")
        reg2 = Testy_Register(SAVES_DIR, "hello")
        reg3 = Testy_Register(SAVES_DIR, "hello")
        with reg3.open(): pass
        with reg2.open() as reg2:
            try:
                reg2.add_subregister(reg3)
            except Subregister_Cycle_Error:
                self.fail()
        with reg1.open() as reg1:
            try:
                reg1.add_subregister(reg2)
            except Subregister_Cycle_Error:
                self.fail()
        with reg3.open() as reg3:
            with self.assertRaises(Subregister_Cycle_Error):
                reg3.add_subregister(reg1)

    def test_remove_subregister(self):

        reg1 = Testy_Register(SAVES_DIR, "hello")
        reg2 = Testy_Register(SAVES_DIR, "hello")
        reg3 = Testy_Register(SAVES_DIR, "hello")
        with reg2.open():pass
        with reg3.open():pass
        with reg1.open():
            reg1.add_subregister(reg2)
            self.assertEqual(
                1,
                leveldb_count_keys(reg1._db, _SUB_KEY_PREFIX)
            )
            reg1.remove_subregister(reg2)
            self.assertEqual(
                0,
                leveldb_count_keys(reg1._db, _SUB_KEY_PREFIX)
            )

            reg1.add_subregister(reg2)
            reg1.add_subregister(reg3)
            self.assertEqual(
                2,
                leveldb_count_keys(reg1._db, _SUB_KEY_PREFIX)
            )
            reg1.remove_subregister(reg2)
            self.assertEqual(
                1,
                leveldb_count_keys(reg1._db, _SUB_KEY_PREFIX)
            )
            reg1.remove_subregister(reg3)
            self.assertEqual(
                0,
                leveldb_count_keys(reg1._db, _SUB_KEY_PREFIX)
            )

    def test_get_all_ram_blocks(self):

        reg = Testy_Register(SAVES_DIR, "whatever")
        apri = Apri_Info(name = "whatev")

        with reg.open() as reg: pass
        with self.assertRaisesRegex(Register_Not_Open_Error, "get_all_ram_blocks"):
            for _ in reg.get_all_ram_blocks(apri, True): pass

        reg = Testy_Register(SAVES_DIR, "whatever")
        apri1 = Apri_Info(name = "foomy")
        apri2 = Apri_Info(name = "doomy")
        blk1 = Block(list(range(10)), apri1)
        blk2 = Block(list(range(20)), apri1, 10)
        blk3 = Block(list(range(14)), apri2, 50)
        blk4 = Block(list(range(100)), apri2, 120)
        blk5 = Block(list(range(120)), apri2, 1000)
        reg1 = Testy_Register(SAVES_DIR, "helllo")
        reg2 = Testy_Register(SAVES_DIR, "suuup")
        reg1.add_ram_block(blk1)
        reg1.add_ram_block(blk2)
        reg1.add_ram_block(blk3)
        reg2.add_ram_block(blk4)
        reg2.add_ram_block(blk5)
        try:
            reg1.get_all_ram_blocks(apri1, True)
        except Register_Not_Open_Error:
            self.fail("_check_open_raise should only be called if data couldn't be found in initial register")

        total = 0
        for i, blk in enumerate(reg1.get_all_ram_blocks(apri1)):
            total += 1
            if i == 0:
                self.assertIs(
                    blk1,
                    blk
                )
            elif i == 1:
                self.assertIs(
                    blk2,
                    blk
                )
            else:
                self.fail()
        self.assertEqual(
            2,
            total
        )


        with reg2.open(): pass
        with reg1.open() as reg1:
            reg1.add_subregister(reg2)
            total = 0
            for i, blk in enumerate(reg1.get_all_ram_blocks(apri1, True)):
                total += 1
                if i == 0:
                    self.assertIs(
                        blk1,
                        blk
                    )
                elif i == 1:
                    self.assertIs(
                        blk2,
                        blk
                    )
                else:
                    self.fail()
            self.assertEqual(
                2,
                total
            )

            total = 0
            for i, blk in enumerate(reg1.get_all_ram_blocks(apri2, True)):
                total += 1
                if i == 0:
                    self.assertIs(
                        blk3,
                        blk
                    )
                elif i == 1:
                    self.assertIs(
                        blk4,
                        blk
                    )
                elif i == 2:
                    self.assertIs(
                        blk5,
                        blk
                    )
                else:
                    self.fail()
            self.assertEqual(
                3,
                total
            )

    def test_get_ram_block_by_n(self):

        reg = Testy_Register(SAVES_DIR, "whatever")

        apri = Apri_Info(name = "whatev")
        with reg.open() as reg: pass
        with self.assertRaisesRegex(Register_Not_Open_Error, "get_ram_block_by_n"):
            for _ in reg.get_ram_block_by_n(apri, 0, True): pass

        apri1 = Apri_Info(name = "foomy")
        apri2 = Apri_Info(name = "doomy")
        blk1 = Block(list(range(10)), apri1)
        blk2 = Block(list(range(20)), apri1, 10)
        blk3 = Block(list(range(14)), apri2, 50)
        blk4 = Block(list(range(100)), apri2, 120)
        blk5 = Block(list(range(120)), apri2, 1000)
        reg1 = Testy_Register(SAVES_DIR, "helllo")
        reg2 = Testy_Register(SAVES_DIR, "suuup")
        reg1.add_ram_block(blk1)
        reg1.add_ram_block(blk2)
        reg1.add_ram_block(blk3)
        reg2.add_ram_block(blk4)
        reg2.add_ram_block(blk5)
        try:
            reg1.get_ram_block_by_n(apri1, 0, True)
        except Register_Not_Open_Error:
            self.fail("_check_open_raise should only be called if data couldn't be found in initial register")

        tests = [
            (reg1, (apri1,    0, True ), blk1),
            (reg1, (apri1,    0, False), blk1),
            (reg1, (apri1,    9, True ), blk1),
            (reg1, (apri1,    9, False), blk1),
            (reg1, (apri1,   10, True ), blk2),
            (reg1, (apri1,   10, False), blk2),
            (reg1, (apri1,   29, True ), blk2),
            (reg1, (apri1,   29, False), blk2),
            (reg1, (apri2,   50, True ), blk3),
            (reg1, (apri2,   50, False), blk3),
            (reg1, (apri2,   63, True ), blk3),
            (reg1, (apri2,   63, False), blk3),
            (reg2, (apri2,  120, True ), blk4),
            (reg2, (apri2,  219, True ), blk4),
            (reg2, (apri2, 1000, True ), blk5),
            (reg2, (apri2, 1119, True ), blk5)
        ]


        for reg, args, blk in tests:
            if args[2]:
                with reg.open() as reg:
                    self.assertIs(
                        blk,
                        reg.get_ram_block_by_n(*args)
                    )
            else:
                self.assertIs(
                    blk,
                    reg.get_ram_block_by_n(*args)
                )

    def test_sequences_calculated(self):pass

    def test__iter_ram_and_disk_block_metadatas(self):pass

    def test_get_all_disk_blocks(self):pass

    def test_get_disk_block_by_n(self):pass

    def test_get_disk_block_by_metadata(self):pass

    def test_get_all_apri_info(self):pass
