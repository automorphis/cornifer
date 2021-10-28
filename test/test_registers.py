import shutil
from itertools import product
from pathlib import Path
from unittest import TestCase

import plyvel

from cornifer import NumPy_Register, Register, Apri_Info, Block
from cornifer.errors import Register_Not_Open_Error, Register_Not_Created_Error, Register_Already_Open_Error, \
    Data_Not_Found_Error
from cornifer.registers import _BLK_KEY_PREFIX, _KEY_SEP, _CLS_KEY, _MSG_KEY, _CURR_ID_KEY, \
    _APRI_ID_KEY_PREFIX, _ID_APRI_KEY_PREFIX, _START_N_HEAD_KEY, _START_N_TAIL_LENGTH_KEY
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
    - _get_apri_by_id
    - _check_no_cycles

- LEVEL 6
    - _convert_disk_block_key (nonzero head)
    - _get_disk_block_key
    - _iter_disk_block_metadatas
    - _from_name (different registers)
    - open

- LEVEL 7
    - _recursive_open
    - get_disk_block_by_metadata
    - remove_disk_block
    - get_all_disk_blocks

- LEVEL 8
    - _check_no_cycles
    - get_disk_block_by_n
    
- LEVEL 9
    - add_subregister
    
- LEVEL 10
    - remove_subregister
    - _iter_subregisters
    
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

        local_dir = SAVES_DIR / "bad" / "test_local_dir"
        reg = Testy_Register(SAVES_DIR, "sup")
        with self.assertRaisesRegex(ValueError, "sub-directory"):
            reg._set_local_dir(local_dir)

        local_dir = SAVES_DIR / "test_local_dir"
        reg = Testy_Register(SAVES_DIR, "sup")
        with self.assertRaises(FileNotFoundError):
            reg._set_local_dir(local_dir)

        local_dir = SAVES_DIR / "test_local_dir"
        reg = Testy_Register(SAVES_DIR, "sup")
        local_dir.mkdir()
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
        self.assertFalse(reg._opened)
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

        reg = NumPy_Register(SAVES_DIR, "msg")
        blk = Block([], Apri_Info(name = "name"))
        reg.add_ram_block(blk)
        try:
            reg.remove_ram_block(blk)
        except Register_Not_Open_Error:
            self.fail("removing ram blocks doesn't need reg to be open")

        reg = NumPy_Register(SAVES_DIR, "msg")
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

    def test_get_ram_block_by_n(self):

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
        for n in [-1, 1000]:
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

        reg2 = Testy_Register(SAVES_DIR, "msg")
        reg2._set_local_dir(reg1._local_dir)
        self.assertEqual(
            hash(reg2),
            hash(reg1)
        )

        reg2 = Testy_Register2(SAVES_DIR, "msg")
        reg2._set_local_dir(reg1._local_dir)
        self.assertNotEqual(
            hash(reg2),
            hash(reg1)
        )

        reg2 = Testy_Register(SAVES_DIR, "msg")
        reg2._set_local_dir(".." / SAVES_DIR / reg1._local_dir)
        self.assertEqual(
            hash(reg2),
            hash(reg1)
        )

    def test___eq___created(self):
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

        reg2 = Testy_Register(SAVES_DIR, "msg")
        reg2._set_local_dir(reg1._local_dir)
        self.assertEqual(
            reg2,
            reg1
        )

        reg2 = Testy_Register2(SAVES_DIR, "msg")
        reg2._set_local_dir(reg1._local_dir)
        self.assertNotEqual(
            reg2,
            reg1
        )

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

        apri = Apri_Info(name = "hi")
        reg = Testy_Register(SAVES_DIR, "hi")
        with self.assertRaises(ValueError):
            reg._get_id_by_apri(None,None)

        apri1 = Apri_Info(name = "hi")
        apri2 = Apri_Info(name = "hello")
        apri3 = Apri_Info(name = "sup")
        reg = Testy_Register(SAVES_DIR, "hi")
        with reg.open() as reg:
            curr_id = reg._db.get(_CURR_ID_KEY)
            _id1 = reg._get_id_by_apri(apri1,None)
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

            _id2 = reg._get_id_by_apri(apri2, None)
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

            _id3 = reg._get_id_by_apri(None, apri3.to_json().encode("ASCII"))
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

    def test__get_instance(self):

        reg1 = Testy_Register(SAVES_DIR, "msg")
        with reg1.open() as reg1: pass
        reg2 = Testy_Register(SAVES_DIR, "msg")
        reg2._set_local_dir(reg1._local_dir)
        self.assertIs(
            reg1,
            Register._get_instance(reg2)
        )
        self.assertIs(
            reg1,
            Register._get_instance(reg1)
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
            _id1 = reg._get_id_by_apri(apri1, None)

            self.assertIsInstance(
                _id1,
                bytes
            )
            self.assertEqual(
                apri1,
                Apri_Info.from_json(reg._get_apri_by_id(_id1).decode("ASCII"))
            )

            apri2 = Apri_Info(name = "sup")
            _id2 = reg._get_id_by_apri(apri2, None)
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
            reg._get_id_by_apri(apri1, None)
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

    def test__from_name_same_register(self):

        reg = Testy_Register(SAVES_DIR, "hello")
        with reg.open() as reg: pass
        with self.assertRaisesRegex(TypeError, "abstract"):
            Register._from_name("Register", reg._local_dir)

        reg = Testy_Register(SAVES_DIR, "hello")
        with reg.open() as reg: pass
        with self.assertRaisesRegex(TypeError, "add_subclass"):
            Register._from_name("Testy_Register2", reg._local_dir)

        reg1 = Testy_Register(SAVES_DIR, "hellooooo")
        with reg1.open() as reg1: pass
        reg2 = Register._from_name("Testy_Register", reg1._local_dir)
        self.assertIs(
            reg1,
            reg2
        )

    def test__open_created(self):

        reg = Testy_Register(SAVES_DIR, "testy")
        with reg.open() as reg: pass
        with reg.open() as reg:
            self.assertTrue(reg._opened)
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
        apri = Apri_Info(name = "hello")
        with reg.open() as reg:
            _id1 = reg._get_id_by_apri(apri,None)
            _id2 = reg._get_id_by_apri(apri,None)
            self.assertIsInstance(
                _id2,
                bytes
            )
            self.assertEqual(
                _id1,
                _id2
            )

            _id3 = reg._get_id_by_apri(None, apri.to_json().encode("ASCII"))
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
            for start_n in [0, head * 10 ** tail_length - 1]:
                reg = Testy_Register(SAVES_DIR, "hello")
                with reg.open() as reg:
                    if head > 0:
                        blk = Block([], Apri_Info(name = "hi"), start_n)
                        reg.add_disk_block(blk)
                        with self.assertRaisesRegex(ValueError, "correct head"):
                            reg.set_start_n_info(head, tail_length)

                        # make sure it exits safely
                        self.assertEqual(
                            10 ** Register._START_N_TAIL_LENGTH_DEFAULT,
                            reg._start_n_tail_mod
                        )
                        self.assertEqual(
                            0,
                            reg._start_n_head
                        )
                        self.assertEqual(
                            Register._START_N_TAIL_LENGTH_DEFAULT,
                            reg._start_n_tail_length
                        )
                        self.assertEqual(
                            b"0",
                            reg._start_n_head_bytes
                        )
                        self.assertEqual(
                            str(Register._START_N_TAIL_LENGTH_DEFAULT).encode("ASCII"),
                            reg._start_n_tail_length_bytes
                        )
                        self.assertEqual(
                            b"0",
                            reg._db.get(_START_N_HEAD_KEY)
                        )
                        self.assertEqual(
                            str(Register._START_N_TAIL_LENGTH_DEFAULT).encode("ASCII"),
                            reg._db.get(_START_N_TAIL_LENGTH_KEY)
                        )

            # test to make sure smallest possible start_n works
            reg = Testy_Register(SAVES_DIR, "hello")
            apri = Apri_Info(name="hi")
            with reg.open() as reg:
                if head > 0:
                    blk = Block([], apri, head * 10 ** tail_length)
                    reg.add_disk_block(blk)
                    try:
                        reg.set_start_n_info(head, tail_length)
                    except ValueError:
                        self.fail()

                    self.assertEqual(
                        10 ** tail_length,
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
                        reg._start_n_head_bytes
                    )
                    self.assertEqual(
                        str(tail_length).encode("ASCII"),
                        reg._start_n_tail_length_bytes
                    )
                    self.assertEqual(
                        reg._start_n_head_bytes,
                        reg._db.get(_START_N_HEAD_KEY)
                    )
                    self.assertEqual(
                        reg._start_n_tail_length_bytes,
                        reg._db.get(_START_N_TAIL_LENGTH_KEY)
                    )

                    with leveldb_prefix_iterator(reg._db, _BLK_KEY_PREFIX) as it:
                        for key,_ in it:pass

                    _apri, start_n, length = reg._convert_disk_block_key(key, None)
                    self.assertEqual(
                        apri,
                        _apri
                    )
                    self.assertEqual(
                        head * 10 ** tail_length,
                        start_n
                    )
                    self.assertEqual(
                        0,
                        length
                    )