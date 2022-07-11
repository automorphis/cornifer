import os
import re
import shutil
from itertools import product, chain
from pathlib import Path
from unittest import TestCase

import lmdb
import numpy as np

from cornifer import Numpy_Register, Register, Apri_Info, Block, Apos_Info
from cornifer.errors import Register_Already_Open_Error, Data_Not_Found_Error, Register_Error, Compression_Error, \
    Decompression_Error
from cornifer.register_file_structure import REGISTER_FILENAME, VERSION_FILEPATH, MSG_FILEPATH, CLS_FILEPATH, \
    DATABASE_FILEPATH
from cornifer.registers import _BLK_KEY_PREFIX, _KEY_SEP, _CLS_KEY, _MSG_KEY, _CURR_ID_KEY, \
    _APRI_ID_KEY_PREFIX, _ID_APRI_KEY_PREFIX, _START_N_HEAD_KEY, _START_N_TAIL_LENGTH_KEY, _SUB_KEY_PREFIX, \
    _COMPRESSED_KEY_PREFIX, _IS_NOT_COMPRESSED_VAL, _BLK_KEY_PREFIX_LEN, _SUB_VAL, _APOS_KEY_PREFIX, \
    _COMPRESSED_KEY_PREFIX_LEN
from cornifer.utilities.lmdb import lmdb_has_key, lmdb_prefix_iterator, lmdb_count_keys, open_lmdb, lmdb_prefix_list
from cornifer.version import CURRENT_VERSION

"""
- LEVEL 0
    - __init__
    - add_subclass
    - _split_disk_block_key
    - _join_disk_block_data

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
    - _get_apri_json_by_id
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
    - _check_no_cycles_from
    - add_subregister
    
- LEVEL 10
    - remove_subregister
    
"""

SAVES_DIR = Path("D:/tmp/tests")
# SAVES_DIR = Path.home() / "tmp" / "tests"

class Testy_Register(Register):

    @classmethod
    def dump_disk_data(cls, data, filename, **kwargs):

        filename.touch()
        return filename

    @classmethod
    def load_disk_data(cls, filename, **kwargs):
        return None

    @classmethod
    def clean_disk_data(cls, filename, **kwargs):

        filename = Path(filename)
        filename.unlink(missing_ok = False)

Register.add_subclass(Testy_Register)

class Testy_Register2(Register):
    @classmethod
    def dump_disk_data(cls, data, filename, **kwargs): pass

    @classmethod
    def load_disk_data(cls, filename, **kwargs): pass

    @classmethod
    def clean_disk_data(cls, filename, **kwargs):pass

def data(blk):
    return blk.get_apri(), blk.get_start_n(), len(blk)

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

        with self.assertRaises(TypeError):
            Testy_Register(0, "sup")

        self.assertFalse(Testy_Register(SAVES_DIR, "sup")._created)

        self.assertEqual(Testy_Register(SAVES_DIR, "sup")._version, CURRENT_VERSION)

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
                Register._split_disk_block_key(_BLK_KEY_PREFIX_LEN, key)
            )
        for key in keys:
            self.assertEqual(
                key,
                Register._join_disk_block_data( *((_BLK_KEY_PREFIX, ) + Register._split_disk_block_key(_BLK_KEY_PREFIX_LEN, key)))
            )

    def test__join_disk_block_data(self):

        splits = [
            (_BLK_KEY_PREFIX, b"hello", b"there", b"friend"),
            (_BLK_KEY_PREFIX, b"",      b"there", b"friend"),
            (_BLK_KEY_PREFIX, b"hello", b"",      b"friend"),
            (_BLK_KEY_PREFIX, b"hello", b"there", b""      ),
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
               Register._join_disk_block_data(*split)
            )
        for split in splits:
            self.assertEqual(
                split[1:],
                Register._split_disk_block_key(_BLK_KEY_PREFIX_LEN, Register._join_disk_block_data(*split))
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

    def test__check_open_raise_uncreated(self):

        reg = Testy_Register(SAVES_DIR, "hey")

        with self.assertRaisesRegex(Register_Error, "test"):
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

        # test that newly created register has the correct filestructure and instance attributes
        # register database must be manually created for this test case
        local_dir = SAVES_DIR / "test_local_dir"
        reg = Testy_Register(SAVES_DIR, "sup")
        local_dir.mkdir()
        (local_dir / REGISTER_FILENAME).mkdir(exist_ok = False)
        (local_dir / VERSION_FILEPATH).touch(exist_ok = False)
        (local_dir / MSG_FILEPATH).touch(exist_ok = False)
        (local_dir / CLS_FILEPATH).touch(exist_ok = False)
        (local_dir / DATABASE_FILEPATH).mkdir(exist_ok = False)

        try:
            reg._db = open_lmdb(local_dir / REGISTER_FILENAME, 1, False)

            reg._set_local_dir(local_dir)

            self.assertTrue(reg._created)

            self.assertEqual(
                local_dir,
                reg._local_dir
            )

            self.assertEqual(
                str(local_dir).encode("ASCII"),
                reg._local_dir_bytes
            )

            self.assertEqual(
                _SUB_KEY_PREFIX + reg._local_dir_bytes,
                reg._subreg_bytes
            )

            self.assertEqual(
                reg._db_filepath,
                local_dir / DATABASE_FILEPATH
            )

        finally:
            reg._db.close()

    def test___hash___uncreated(self):
        with self.assertRaisesRegex(Register_Error, "__hash__"):
            hash(Testy_Register(SAVES_DIR, "hey"))

    def test___eq___uncreated(self):
        with self.assertRaises(Register_Error):
            Testy_Register(SAVES_DIR, "hey") == Testy_Register(SAVES_DIR, "sup")

    def test_add_ram_block(self):

        reg = Testy_Register(SAVES_DIR, "msg")
        blk = Block([], Apri_Info(name = "test"))
        try:
            reg.add_ram_block(blk)
        except Register_Error:
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

        with reg.open() as reg:
            self.assertFalse(reg._db_is_closed())

        self.assertTrue(reg._created)

        keyvals = {
            _START_N_HEAD_KEY : b"0",
            _START_N_TAIL_LENGTH_KEY : str(Register._START_N_TAIL_LENGTH_DEFAULT).encode("ASCII"),
            _CURR_ID_KEY: b"0"
        }

        self.assertTrue(reg._db_is_closed())

        db = None

        try:

            db = open_lmdb(reg._db_filepath, 1, False)

            with db.begin() as txn:
                for key, val in keyvals.items():
                    self.assertEqual(
                        val,
                        txn.get(key)
                    )

            self.assertEqual(
                len(keyvals),
                lmdb_count_keys(db, b"")
            )

        finally:
            if db is not None:
                db.close()

    def test_remove_ram_block(self):

        reg = Numpy_Register(SAVES_DIR, "msg")
        blk = Block([], Apri_Info(name = "name"))
        reg.add_ram_block(blk)
        try:
            reg.remove_ram_block(blk)
        except Register_Error:
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
        except Register_Error:
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
        except Register_Error:
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
        with self.assertRaisesRegex(Register_Error, "xyz"):
            reg._check_open_raise("xyz")

        reg = Testy_Register(SAVES_DIR, "hi")
        with reg.open() as reg:
            try:
                reg._check_open_raise("xyz")
            except Register_Error:
                self.fail("the register is open")

        reg = Testy_Register(SAVES_DIR, "hi")
        with reg.open() as reg:pass
        with self.assertRaisesRegex(Register_Error, "xyz"):
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

            with reg._db.begin() as txn:
                curr_id = txn.get(_CURR_ID_KEY)

            _id1 = reg._get_id_by_apri(apri1, None, True)
            self.assertEqual(
                curr_id,
                _id1
            )

            self.assertEqual(
                1,
                lmdb_count_keys(reg._db, _APRI_ID_KEY_PREFIX)
            )

            self.assertEqual(
                1,
                lmdb_count_keys(reg._db, _ID_APRI_KEY_PREFIX)
            )

            _id2 = reg._get_id_by_apri(apri2, None, True)
            self.assertNotEqual(
                _id1,
                _id2
            )
            self.assertEqual(
                2,
                lmdb_count_keys(reg._db, _APRI_ID_KEY_PREFIX)
            )
            self.assertEqual(
                2,
                lmdb_count_keys(reg._db, _ID_APRI_KEY_PREFIX)
            )

            _id3 = reg._get_id_by_apri(None, apri3.to_json().encode("ASCII"), True)
            self.assertNotIn(
                _id3,
                [_id1, _id2]
            )
            self.assertEqual(
                3,
                lmdb_count_keys(reg._db, _APRI_ID_KEY_PREFIX)
            )
            self.assertEqual(
                3,
                lmdb_count_keys(reg._db, _ID_APRI_KEY_PREFIX)
            )

            with self.assertRaises(Data_Not_Found_Error):
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

        try:
            reg.set_message("yes")

        except Register_Error as e:
            if "has not been opened" in str(e):
                self.fail("the register doesn't need to be open for set_message")
            else:
                raise e

        self.assertEqual(
            "yes",
            str(reg)
        )

        with reg.open() as reg:pass

        reg.set_message("no")

        self.assertEqual(
            "no",
            str(reg)
        )

        with reg._msg_filepath.open("r") as fh:
            self.assertEqual(
                "no",
                fh.read()
            )

    def test_add_disk_block(self):

        reg = Testy_Register(SAVES_DIR, "sup")
        blk = Block([], Apri_Info(name = "hi"))
        with self.assertRaisesRegex(Register_Error, "open.*add_disk_block"):
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
        blk5 = Block([], Apri_Info(sir = "hey", maam = "hi"))
        blk6 = Block([], Apri_Info(maam="hi", sir = "hey"))
        with reg.open() as reg:

            reg.add_disk_block(blk1)
            self.assertEqual(
                1,
                lmdb_count_keys(reg._db, _BLK_KEY_PREFIX)
            )
            self.assertEqual(
                1,
                lmdb_count_keys(reg._db, _APRI_ID_KEY_PREFIX)
            )
            self.assertEqual(
                1,
                lmdb_count_keys(reg._db, _ID_APRI_KEY_PREFIX)
            )

            reg.add_disk_block(blk2)
            self.assertEqual(
                2,
                lmdb_count_keys(reg._db, _BLK_KEY_PREFIX)
            )
            self.assertEqual(
                1,
                lmdb_count_keys(reg._db, _APRI_ID_KEY_PREFIX)
            )
            self.assertEqual(
                1,
                lmdb_count_keys(reg._db, _ID_APRI_KEY_PREFIX)
            )

            reg.add_disk_block(blk3)
            self.assertEqual(
                3,
                lmdb_count_keys(reg._db, _BLK_KEY_PREFIX)
            )
            self.assertEqual(
                2,
                lmdb_count_keys(reg._db, _APRI_ID_KEY_PREFIX)
            )
            self.assertEqual(
                2,
                lmdb_count_keys(reg._db, _ID_APRI_KEY_PREFIX)
            )
            with self.assertRaisesRegex(Register_Error, "[dD]uplicate"):
                reg.add_disk_block(blk4)

            reg.add_disk_block(blk5)

            with self.assertRaisesRegex(Register_Error, "[dD]uplicate"):
                reg.add_disk_block(blk6)

        with self.assertRaisesRegex(Register_Error, "read-only"):
            with reg.open(read_only = True) as reg:
                reg.add_disk_block(blk)

        reg = Numpy_Register(SAVES_DIR, "no")

        with reg.open() as reg:

            reg.add_disk_block(Block(np.arange(30), Apri_Info(maybe = "maybe")))

            for debug in [1,2,3,4]:

                apri = Apri_Info(none = "all")
                blk = Block(np.arange(14), apri, 0)

                with self.assertRaises(KeyboardInterrupt):
                    reg.add_disk_block(blk, debug = debug)

                self.assertEqual(
                    1,
                    lmdb_count_keys(reg._db, _BLK_KEY_PREFIX)
                )

                self.assertEqual(
                    1,
                    lmdb_count_keys(reg._db, _COMPRESSED_KEY_PREFIX)
                )

                self.assertEqual(
                    1,
                    lmdb_count_keys(reg._db, _ID_APRI_KEY_PREFIX)
                )

                self.assertEqual(
                    1,
                    lmdb_count_keys(reg._db, _APRI_ID_KEY_PREFIX)
                )

                self.assertEqual(
                    1,
                    sum(1 for d in reg._local_dir.iterdir() if d.is_file())
                )

                self.assertTrue(np.all(
                    np.arange(30) ==
                    reg.get_disk_block(Apri_Info(maybe = "maybe"), 0, 30).get_segment()
                ))

                with self.assertRaises(Data_Not_Found_Error):
                    reg.get_disk_block(Apri_Info(none = "all"), 0, 14)

    def test__get_apri_json_by_id(self):

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
                Apri_Info.from_json(reg._get_apri_json_by_id(_id1).decode("ASCII"))
            )

            apri2 = Apri_Info(name = "sup")
            _id2 = reg._get_id_by_apri(apri2, None, True)
            self.assertEqual(
                apri2,
                Apri_Info.from_json(reg._get_apri_json_by_id(_id2).decode("ASCII"))
            )

    def test_get_all_apri_info_no_recursive(self):

        reg = Testy_Register(SAVES_DIR, "msg")
        with self.assertRaisesRegex(Register_Error, "get_all_apri_info"):
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
            self.assertFalse(reg._db_is_closed())
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
            with lmdb_prefix_iterator(reg._db, _BLK_KEY_PREFIX) as it:
                for curr_key,_ in it: pass
            self.assertEqual(
                (apri1, 0, 0),
                reg._convert_disk_block_key(_BLK_KEY_PREFIX_LEN, curr_key)
            )
            self.assertEqual(
                (apri1, 0, 0),
                reg._convert_disk_block_key(_BLK_KEY_PREFIX_LEN, curr_key, apri1)
            )
            old_keys = {curr_key}

            blk2 = Block(list(range(10)), apri1)
            reg.add_disk_block(blk2)
            with lmdb_prefix_iterator(reg._db, _BLK_KEY_PREFIX) as it:
                for key,_val in it:
                    if key not in old_keys:
                        curr_key = key
            self.assertEqual(
                (apri1, 0, 10),
                reg._convert_disk_block_key(_BLK_KEY_PREFIX_LEN, curr_key)
            )
            old_keys.add(curr_key)

            apri2 = Apri_Info(name = "hello")
            blk3 = Block(list(range(100)), apri2, 10)
            reg.add_disk_block(blk3)
            with lmdb_prefix_iterator(reg._db, _BLK_KEY_PREFIX) as it:
                for key,_val in it:
                    if key not in old_keys:
                        curr_key = key
            self.assertEqual(
                (apri2, 10, 100),
                reg._convert_disk_block_key(_BLK_KEY_PREFIX_LEN, curr_key)
            )
            old_keys.add(curr_key)

            blk4 = Block(list(range(100)), apri2)
            reg.add_disk_block(blk4)
            with lmdb_prefix_iterator(reg._db, _BLK_KEY_PREFIX) as it:
                for key,_val in it:
                    if key not in old_keys:
                        curr_key = key
            self.assertEqual(
                (apri2, 0, 100),
                reg._convert_disk_block_key(_BLK_KEY_PREFIX_LEN, curr_key)
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

        with reg._db.begin() as txn:

            self.assertEqual(
                str(head).encode("ASCII"),
                txn.get(_START_N_HEAD_KEY)
            )

            self.assertEqual(
                str(tail_length).encode("ASCII"),
                txn.get(_START_N_TAIL_LENGTH_KEY)
            )

    def check_key_set_start_n_info(self, reg, key, apri, start_n, length):
        _apri, _start_n, _length = reg._convert_disk_block_key(_BLK_KEY_PREFIX_LEN, key, None)
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
        with self.assertRaisesRegex(Register_Error, "set_start_n_info"):
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
            with self.assertRaisesRegex(ValueError, "positive"):
                reg.set_start_n_info(0, -1)

        reg = Testy_Register(SAVES_DIR, "hello")
        with reg.open() as reg:
            with self.assertRaisesRegex(ValueError, "positive"):
                reg.set_start_n_info(0, 0)


        for head, tail_length in product([0, 1, 10, 100, 1100, 450], [1,2,3,4,5]):

            # check set works
            reg = Testy_Register(SAVES_DIR, "hello")
            with reg.open() as reg:

                try:
                    reg.set_start_n_info(head, tail_length)

                except ValueError:
                    self.fail(f"head = {head}, tail_length = {tail_length} are okay")

                with reg._db.begin() as txn:
                    self.assertEqual(
                        str(head).encode("ASCII"),
                        txn.get(_START_N_HEAD_KEY)
                    )

                    self.assertEqual(
                        str(tail_length).encode("ASCII"),
                        txn.get(_START_N_TAIL_LENGTH_KEY)
                    )

            # check read-only mode doesn't work
            with reg.open(read_only = True) as reg:
                with self.assertRaisesRegex(Register_Error, "read-only"):
                    reg.set_start_n_info(head, tail_length)

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

                    for debug in [0, 1, 2]:

                        if debug == 0:
                            reg.set_start_n_info(head, tail_length)

                        else:
                            with self.assertRaises(KeyboardInterrupt):
                                reg.set_start_n_info(head // 10, tail_length + 1, debug)

                        self.check_reg_set_start_n_info(
                            reg,
                            10 ** tail_length, head, tail_length
                        )

                        with lmdb_prefix_iterator(reg._db, _BLK_KEY_PREFIX) as it:
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

    def check__iter_disk_block_pairs(self, t, apri, start_n, length):
        self.assertEqual(
            3,
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

    def test__iter_disk_block_pairs(self):

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
                enumerate(reg._iter_disk_block_pairs(_BLK_KEY_PREFIX, None, None)),
                enumerate(reg._iter_disk_block_pairs(_BLK_KEY_PREFIX, apri1, None)),
                enumerate(reg._iter_disk_block_pairs(_BLK_KEY_PREFIX, None, apri1.to_json().encode("ASCII")))
            ):
                total += 1
                if i == 0:
                    t = reg._convert_disk_block_key(_BLK_KEY_PREFIX_LEN, t[0], apri1)
                    self.check__iter_disk_block_pairs(t, apri1, 0, 50)
                else:
                    self.fail()
            if total != 3:
                self.fail(str(total))

            reg.add_disk_block(blk2)
            total = 0
            for i, t in chain(
                enumerate(reg._iter_disk_block_pairs(_BLK_KEY_PREFIX, None, None)),
                enumerate(reg._iter_disk_block_pairs(_BLK_KEY_PREFIX, apri1, None)),
                enumerate(reg._iter_disk_block_pairs(_BLK_KEY_PREFIX, None, apri1.to_json().encode("ASCII")))
            ):
                total += 1
                if i == 0:
                    t = reg._convert_disk_block_key(_BLK_KEY_PREFIX_LEN, t[0], apri1)
                    self.check__iter_disk_block_pairs(t, apri1, 0, 50)
                elif i == 1:
                    t = reg._convert_disk_block_key(_BLK_KEY_PREFIX_LEN, t[0], apri1)
                    self.check__iter_disk_block_pairs(t, apri1, 50, 50)
                else:
                    self.fail()
            if total != 6:
                self.fail(str(total))

            reg.add_disk_block(blk3)
            total = 0
            for i, t in chain(
                enumerate(reg._iter_disk_block_pairs(_BLK_KEY_PREFIX, None, None))
            ):
                total += 1
                if i == 0:
                    t = reg._convert_disk_block_key(_BLK_KEY_PREFIX_LEN, t[0], apri1)
                    self.check__iter_disk_block_pairs(t, apri1, 0, 50)
                elif i == 1:
                    t = reg._convert_disk_block_key(_BLK_KEY_PREFIX_LEN, t[0], apri1)
                    self.check__iter_disk_block_pairs(t, apri1, 50, 50)
                elif i == 2:
                    t = reg._convert_disk_block_key(_BLK_KEY_PREFIX_LEN, t[0], apri2)
                    self.check__iter_disk_block_pairs(t, apri2, 1000, 500)
                else:
                    self.fail()
            if total != 3:
                self.fail()

            total = 0
            for i, t in chain(
                enumerate(reg._iter_disk_block_pairs(_BLK_KEY_PREFIX, apri1, None)),
                enumerate(reg._iter_disk_block_pairs(_BLK_KEY_PREFIX, None, apri1.to_json().encode("ASCII")))
            ):
                total += 1
                if i == 0:
                    t = reg._convert_disk_block_key(_BLK_KEY_PREFIX_LEN, t[0], apri1)
                    self.check__iter_disk_block_pairs(t, apri1, 0, 50)
                elif i == 1:
                    t = reg._convert_disk_block_key(_BLK_KEY_PREFIX_LEN, t[0], apri1)
                    self.check__iter_disk_block_pairs(t, apri1, 50, 50)
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

        reg4 = Testy_Register(SAVES_DIR, "sup")
        with self.assertRaisesRegex(ValueError, "read-only"):
            with reg4.open(read_only = True) as reg:pass

    def test__recursive_open(self):

        # must be created
        reg1 = Testy_Register(SAVES_DIR, "hello")

        with self.assertRaises(Register_Error):
            with reg1._recursive_open(False):pass

        # must be created
        reg2 = Testy_Register(SAVES_DIR, "hello")
        with reg2.open() as reg2:pass
        with reg2._recursive_open(False) as reg3:pass

        self.assertIs(
            reg2,
            reg3
        )

        reg3 = Testy_Register(SAVES_DIR, "hello")
        reg3._set_local_dir(reg2._local_dir)
        with reg3._recursive_open(False) as reg4:pass

        self.assertIs(
            reg2,
            reg4
        )

        reg5 = Testy_Register(SAVES_DIR, "hi")

        with reg5.open() as reg5:

            try:
                with reg5._recursive_open(False):pass

            except Register_Error:
                self.fail()

            else:
                self.assertFalse(
                    reg5._db_is_closed()
                )

        self.assertTrue(
            reg5._db_is_closed()
        )

        reg6 = Testy_Register(SAVES_DIR, "supp")

        with reg6.open() as reg6: pass

        with reg6.open(read_only = True) as reg6:

            with self.assertRaisesRegex(ValueError, "read-only"):
                with reg6._recursive_open(False):pass

    def test_get_disk_block_no_recursive(self):

        reg = Numpy_Register(SAVES_DIR, "hello")
        with self.assertRaisesRegex(Register_Error, "get_disk_block"):
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

            apri3 = Apri_Info(
                name = "'''i love quotes'''and'' backslashes\\\\",
                num = '\\\"double\\quotes\' are cool too"'
            )
            blk = Block(np.arange(69, 420), apri3)
            reg.add_disk_block(blk)

            self.assertEqual(
                blk,
                reg.get_disk_block(apri3, 0, 420 - 69)
            )

    def _remove_disk_block_helper(self, reg, block_data):

        expected_num_blocks = len(block_data)

        self.assertEqual(
            expected_num_blocks,
            lmdb_count_keys(reg._db, _BLK_KEY_PREFIX)
        )

        self.assertEqual(
            expected_num_blocks,
            lmdb_count_keys(reg._db, _COMPRESSED_KEY_PREFIX)
        )

        self.assertEqual(
            sum(d.is_dir() for d in reg._local_dir.iterdir()),
            1
        )

        self.assertEqual(
            sum(d.is_file() for d in reg._local_dir.iterdir()),
            expected_num_blocks
        )

        for apri, start_n, length in block_data:
            key = reg._get_disk_block_key(_BLK_KEY_PREFIX, apri, None, start_n, length, False)
            with reg._db.begin() as txn:
                filename = Path(txn.get(key).decode("ASCII"))
            self.assertTrue((reg._local_dir / filename).exists())

    def test_remove_disk_block(self):

        reg1 = Testy_Register(SAVES_DIR, "hi")

        with self.assertRaisesRegex(Register_Error, "open.*remove_disk_block"):
            reg1.remove_disk_block(Apri_Info(name = "fooopy doooopy"), 0, 0)

        with reg1.open() as reg1:

            apri1 = Apri_Info(name = "fooopy doooopy")
            blk1 = Block(list(range(50)), apri1)
            reg1.add_disk_block(blk1)
            self._remove_disk_block_helper(reg1, [(apri1, 0, 50)])

            reg1.remove_disk_block(apri1, 0, 50)
            self._remove_disk_block_helper(reg1, [])

            reg1.add_disk_block(blk1)
            apri2 = Apri_Info(name = "fooopy doooopy2")
            blk2 = Block(list(range(100)), apri2, 1000)
            reg1.add_disk_block(blk2)
            self._remove_disk_block_helper(reg1, [(apri1, 0, 50), (apri2, 1000, 100)])

            reg1.remove_disk_block(apri2, 1000, 100)
            self._remove_disk_block_helper(reg1, [(apri1, 0, 50)])

            reg1.remove_disk_block(apri1, 0, 50)
            self._remove_disk_block_helper(reg1, [])

        with self.assertRaisesRegex(Register_Error, "read-write"):
            with reg1.open(read_only = True) as reg1:
                reg1.remove_disk_block(apri1, 0, 0)

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
            self._remove_disk_block_helper(reg1, [])

        with reg2.open() as reg2:
            self._remove_disk_block_helper(reg2, [(apri, 0, 0)])

        reg = Numpy_Register(SAVES_DIR, "hello")

        with reg.open() as reg:

            apri = Apri_Info(no = "yes")
            blk = Block(np.arange(14), apri)

            reg.add_disk_block(blk)

            apri = Apri_Info(maybe = "maybe")
            blk = Block(np.arange(20), apri)

            reg.add_disk_block(blk)

            for debug in [1,2,3]:

                if debug == 3:
                    reg.compress(Apri_Info(maybe = "maybe"), 0, 20)

                with self.assertRaises(KeyboardInterrupt):
                    reg.remove_disk_block(Apri_Info(maybe = "maybe"), 0, 20, debug = debug)

                self.assertEqual(
                    2,
                    lmdb_count_keys(reg._db, _BLK_KEY_PREFIX)
                )

                self.assertEqual(
                    2,
                    lmdb_count_keys(reg._db, _COMPRESSED_KEY_PREFIX)
                )

                self.assertEqual(
                    2,
                    lmdb_count_keys(reg._db, _ID_APRI_KEY_PREFIX)
                )

                self.assertEqual(
                    2,
                    lmdb_count_keys(reg._db, _APRI_ID_KEY_PREFIX)
                )

                self.assertEqual(
                    2 + (1 if debug == 3 else 0),
                    sum(1 for d in reg._local_dir.iterdir() if d.is_file())
                )

                if debug == 3:
                    reg.decompress(Apri_Info(maybe = "maybe"), 0, 20)

                self.assertTrue(np.all(
                    np.arange(14) ==
                    reg.get_disk_block(Apri_Info(no = "yes"), 0, 14).get_segment()
                ))

                self.assertTrue(np.all(
                    np.arange(20) ==
                    reg.get_disk_block(Apri_Info(maybe = "maybe"), 0, 20).get_segment()
                ))

    def test_set_apos_info(self):

        reg = Testy_Register(SAVES_DIR, "hello")

        with self.assertRaisesRegex(Register_Error, "open.*set_apos_info"):
            reg.set_apos_info(Apri_Info(no = "no"), Apos_Info(yes = "yes"))

        with reg.open() as reg:

            try:
                reg.set_apos_info(Apri_Info(no = "no"), Apos_Info(yes = "yes"))

            except Data_Not_Found_Error:
                self.fail("Do not need apri_info to already be there to add apos_info")

            except Exception as e:
                raise e

            self.assertEqual(
                1,
                lmdb_count_keys(reg._db, _APOS_KEY_PREFIX)
            )

            reg.set_apos_info(Apri_Info(no="no"), Apos_Info(maybe="maybe"))

            self.assertEqual(
                1,
                lmdb_count_keys(reg._db, _APOS_KEY_PREFIX)
            )

            reg.set_apos_info(Apri_Info(weird="right"), Apos_Info(maybe="maybe"))

            self.assertEqual(
                2,
                lmdb_count_keys(reg._db, _APOS_KEY_PREFIX)
            )

            reg.set_apos_info(Apri_Info(weird="right"), Apos_Info(maybe="maybe"))

            self.assertEqual(
                2,
                lmdb_count_keys(reg._db, _APOS_KEY_PREFIX)
            )

            for debug in [1,2]:

                with self.assertRaises(KeyboardInterrupt):
                    reg.set_apos_info(Apri_Info(__ = "____"), Apos_Info(eight = 9), debug)

                self.assertEqual(
                    2,
                    lmdb_count_keys(reg._db, _APOS_KEY_PREFIX)
                )

        with reg.open(read_only = True) as reg:
            with self.assertRaisesRegex(Register_Error, "read-write"):
                reg.set_apos_info(Apri_Info(no="no"), Apos_Info(yes="yes"))

    def test_get_apos_info(self):

        reg = Testy_Register(SAVES_DIR, "hello")

        with self.assertRaisesRegex(Register_Error, "open.*get_apos_info"):
            reg.get_apos_info(Apri_Info(no = "no"))

        with reg.open() as reg:

            apri = Apri_Info(no = "yes")
            apos = Apos_Info(yes = "no")

            with self.assertRaisesRegex(Data_Not_Found_Error, re.escape(str(apri))):
                reg.get_apos_info(apri)

            reg.set_apos_info(apri, apos)

            self.assertEqual(
                apos,
                reg.get_apos_info(apri)
            )

            apri = Apri_Info(no = "yes")
            apos = Apos_Info(yes = "no", restart = Apos_Info(num = 1))

            reg.set_apos_info(apri, apos)

            self.assertEqual(
                apos,
                reg.get_apos_info(apri)
            )

        with reg.open(read_only = True) as reg:

            try:
                self.assertEqual(
                    apos,
                    reg.get_apos_info(apri)
                )

            except Register_Error as e:

                if "read-write" in str(e):
                    self.fail("get_apos_info allows the register to be in read-only mode")

                else:
                    raise e

            except Exception as e:
                raise e

    def test_remove_apos_info(self):

        reg = Testy_Register(SAVES_DIR, "hello")

        with self.assertRaisesRegex(Register_Error, "open.*remove_apos_info"):
            reg.remove_apos_info(Apri_Info(no = "no"))

        with reg.open() as reg:

            apri1 = Apri_Info(no = "yes")
            apos1 = Apos_Info(yes = "no")

            apri2 = Apri_Info(maam = "sir")
            apos2 = Apos_Info(sir = "maam", restart = apos1)


            reg.set_apos_info(apri1, apos1)

            reg.remove_apos_info(apri1)

            self.assertEqual(
                0,
                lmdb_count_keys(reg._db, _APOS_KEY_PREFIX)
            )

            with self.assertRaisesRegex(Data_Not_Found_Error, re.escape(str(apri1))):
                reg.get_apos_info(apri1)

            reg.set_apos_info(apri1, apos1)
            reg.set_apos_info(apri2, apos2)

            reg.remove_apos_info(apri2)

            self.assertEqual(
                1,
                lmdb_count_keys(reg._db, _APOS_KEY_PREFIX)
            )

            with self.assertRaisesRegex(Data_Not_Found_Error, re.escape(str(apri2))):
                reg.get_apos_info(apri2)

            self.assertEqual(
                apos1,
                reg.get_apos_info(apri1)
            )

            for debug in [1,2]:

                with self.assertRaises(KeyboardInterrupt):
                    reg.remove_apos_info(apri1, debug)

                self.assertEqual(
                    1,
                    lmdb_count_keys(reg._db, _APOS_KEY_PREFIX)
                )

                self.assertEqual(
                    apos1,
                    reg.get_apos_info(apri1)
                )



        with reg.open(read_only = True) as reg:
            with self.assertRaisesRegex(Register_Error, "read-write"):
                reg.remove_apos_info(apri1)

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

            with reg._db.begin(write = True) as txn:
                txn.put(reg._get_subregister_key(), _SUB_VAL)

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

            with reg1._db.begin(write=True) as txn:
                txn.put(reg2._get_subregister_key(), _SUB_VAL)
                txn.put(reg3._get_subregister_key(), _SUB_VAL)

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

            with reg2._db.begin(write=True) as txn:
                txn.put(reg3._get_subregister_key(), _SUB_VAL)

        with reg1.open() as reg:

            with reg1._db.begin(write=True) as txn:
                txn.put(reg2._get_subregister_key(), _SUB_VAL)

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
        with self.assertRaises(Register_Error):
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
                with self.assertRaisesRegex(ValueError, "non-negative"):
                    reg.get_disk_block_by_n(apri1, n)
            for n in [200, 201, 1000]:
                with self.assertRaises(Data_Not_Found_Error):
                    reg.get_disk_block_by_n(apri1, n)

    def test__check_no_cycles_from(self):

        reg = Testy_Register(SAVES_DIR, "hello")
        with self.assertRaises(Register_Error):
            reg._check_no_cycles_from(reg)

        reg = Testy_Register(SAVES_DIR, "hello")
        with reg.open() as reg:pass

        # loop
        self.assertFalse(
            reg._check_no_cycles_from(reg)
        )

        reg1 = Testy_Register(SAVES_DIR, "hello")
        reg2 = Testy_Register(SAVES_DIR, "hello")
        reg3 = Testy_Register(SAVES_DIR, "hello")
        reg4 = Testy_Register(SAVES_DIR, "hello")
        reg5 = Testy_Register(SAVES_DIR, "hello")
        reg6 = Testy_Register(SAVES_DIR, "hello")
        reg7 = Testy_Register(SAVES_DIR, "hello")
        with reg1.open(): pass
        with reg2.open(): pass
        with reg3.open(): pass
        with reg4.open(): pass
        with reg5.open(): pass
        with reg6.open(): pass
        with reg7.open(): pass

        # disjoint
        self.assertTrue(
            reg2._check_no_cycles_from(reg1)
        )

        # 1-path (1 -> 2)
        with reg1.open() as reg1:
            with reg1._db.begin(write = True) as txn:
                txn.put(reg2._get_subregister_key(), _SUB_VAL)

        self.assertFalse(
            reg1._check_no_cycles_from(reg2)
        )

        self.assertTrue(
            reg2._check_no_cycles_from(reg1)
        )

        self.assertFalse(
            reg2._check_no_cycles_from(reg2)
        )

        self.assertFalse(
            reg1._check_no_cycles_from(reg1)
        )

        self.assertTrue(
            reg3._check_no_cycles_from(reg2)
        )

        self.assertTrue(
            reg2._check_no_cycles_from(reg3)
        )

        self.assertTrue(
            reg3._check_no_cycles_from(reg1)
        )

        self.assertTrue(
            reg1._check_no_cycles_from(reg3)
        )

        # 2-path (1 -> 2 -> 3)
        with reg2.open() as reg2:
            with reg2._db.begin(write=True) as txn:
                txn.put(reg3._get_subregister_key(), _SUB_VAL)

        self.assertFalse(
            reg1._check_no_cycles_from(reg1)
        )

        self.assertFalse(
            reg2._check_no_cycles_from(reg2)
        )

        self.assertFalse(
            reg3._check_no_cycles_from(reg3)
        )

        self.assertFalse(
            reg1._check_no_cycles_from(reg2)
        )

        self.assertTrue(
            reg2._check_no_cycles_from(reg1)
        )

        self.assertFalse(
            reg1._check_no_cycles_from(reg3)
        )

        self.assertTrue(
            reg3._check_no_cycles_from(reg1)
        )

        self.assertFalse(
            reg2._check_no_cycles_from(reg3)
        )

        self.assertTrue(
            reg3._check_no_cycles_from(reg2)
        )

        self.assertTrue(
            reg4._check_no_cycles_from(reg1)
        )

        self.assertTrue(
            reg4._check_no_cycles_from(reg2)
        )

        self.assertTrue(
            reg4._check_no_cycles_from(reg3)
        )

        self.assertTrue(
            reg1._check_no_cycles_from(reg4)
        )

        self.assertTrue(
            reg2._check_no_cycles_from(reg4)
        )

        self.assertTrue(
            reg3._check_no_cycles_from(reg4)
        )


        # 2-cycle (4 -> 5 -> 4)

        with reg4.open() as reg4:

            with reg4._db.begin(write = True) as txn:
                txn.put(reg5._get_subregister_key(), _SUB_VAL)

        with reg5.open() as reg5:

            with reg5._db.begin(write=True) as txn:
                txn.put(reg4._get_subregister_key(), _SUB_VAL)

        self.assertFalse(
            reg4._check_no_cycles_from(reg4)
        )

        self.assertFalse(
            reg5._check_no_cycles_from(reg5)
        )

        self.assertFalse(
            reg4._check_no_cycles_from(reg5)
        )

        self.assertFalse(
            reg5._check_no_cycles_from(reg4)
        )

        self.assertTrue(
            reg6._check_no_cycles_from(reg5)
        )

        self.assertTrue(
            reg6._check_no_cycles_from(reg4)
        )

        self.assertTrue(
            reg5._check_no_cycles_from(reg6)
        )

        self.assertTrue(
            reg4._check_no_cycles_from(reg6)
        )

        # 2 cycle with tail (4 -> 5 -> 4 -> 6)

        with reg4.open() as reg4:

            with reg4._db.begin(write = True) as txn:
                txn.put(reg6._get_subregister_key(), _SUB_VAL)

        self.assertFalse(
            reg4._check_no_cycles_from(reg4)
        )

        self.assertFalse(
            reg5._check_no_cycles_from(reg5)
        )

        self.assertFalse(
            reg6._check_no_cycles_from(reg6)
        )

        self.assertFalse(
            reg4._check_no_cycles_from(reg5)
        )

        self.assertFalse(
            reg5._check_no_cycles_from(reg4)
        )

        self.assertFalse(
            reg4._check_no_cycles_from(reg6)
        )

        self.assertTrue(
            reg6._check_no_cycles_from(reg4)
        )

        self.assertFalse(
            reg5._check_no_cycles_from(reg6)
        )

        self.assertTrue(
            reg6._check_no_cycles_from(reg5)
        )

        self.assertTrue(
            reg7._check_no_cycles_from(reg4)
        )

        self.assertTrue(
            reg7._check_no_cycles_from(reg5)
        )

        self.assertTrue(
            reg7._check_no_cycles_from(reg6)
        )

        self.assertTrue(
            reg4._check_no_cycles_from(reg7)
        )

        self.assertTrue(
            reg5._check_no_cycles_from(reg7)
        )

        self.assertTrue(
            reg6._check_no_cycles_from(reg7)
        )

        # 3-cycle (1 -> 2 -> 3 -> 1)

        with reg3.open() as reg2:
            with reg3._db.begin(write=True) as txn:
                txn.put(reg1._get_subregister_key(), _SUB_VAL)

        self.assertFalse(
            reg1._check_no_cycles_from(reg1)
        )

        self.assertFalse(
            reg2._check_no_cycles_from(reg2)
        )

        self.assertFalse(
            reg3._check_no_cycles_from(reg3)
        )

        self.assertFalse(
            reg1._check_no_cycles_from(reg2)
        )

        self.assertFalse(
            reg2._check_no_cycles_from(reg1)
        )

        self.assertFalse(
            reg1._check_no_cycles_from(reg3)
        )

        self.assertFalse(
            reg3._check_no_cycles_from(reg1)
        )

        self.assertFalse(
            reg2._check_no_cycles_from(reg3)
        )

        self.assertFalse(
            reg3._check_no_cycles_from(reg2)
        )

        self.assertTrue(
            reg7._check_no_cycles_from(reg1)
        )

        self.assertTrue(
            reg7._check_no_cycles_from(reg2)
        )

        self.assertTrue(
            reg7._check_no_cycles_from(reg3)
        )

        self.assertTrue(
            reg1._check_no_cycles_from(reg7)
        )

        self.assertTrue(
            reg2._check_no_cycles_from(reg7)
        )

        self.assertTrue(
            reg3._check_no_cycles_from(reg7)
        )

        # long path (0 -> 1 -> ... -> N)

        N = 10

        regs = [Numpy_Register(SAVES_DIR, f"{i}") for i in range(N+2)]

        for reg in regs:
            with reg.open():pass

        for i in range(N):
            with regs[i].open() as reg:
                with reg._db.begin(write=True) as txn:
                    txn.put(regs[i+1]._get_subregister_key(), _SUB_VAL)

        for i, j in product(range(N+1), repeat = 2):

            val = regs[i]._check_no_cycles_from(regs[j])

            if i == j:
                self.assertFalse(val)

            elif i > j:
                self.assertTrue(val)

            else:
                self.assertFalse(val)

        for i in range(N+1):

            self.assertTrue(
                regs[i]._check_no_cycles_from(regs[N+1])
            )

            self.assertTrue(
                regs[N+1]._check_no_cycles_from(regs[i])
            )

        # adding arc between 2 cycle with tail (4 -> 5 -> 4 -> 6) to 3-cycle (1 -> 2 -> 3 -> 1)

        for i, j in product([1,2,3], [4,5,6]):

            regi = eval(f"reg{i}")
            regj = eval(f"reg{j}")

            self.assertTrue(regi._check_no_cycles_from(regj))

    def test_add_subregister(self):

        reg1 = Testy_Register(SAVES_DIR, "hello")
        reg2 = Testy_Register(SAVES_DIR, "hello")
        with self.assertRaisesRegex(Register_Error, "open.*add_subregister"):
            reg1.add_subregister(reg2)

        reg1 = Testy_Register(SAVES_DIR, "hello")
        reg2 = Testy_Register(SAVES_DIR, "hello")
        with reg1.open() as reg1:
            with self.assertRaisesRegex(Register_Error, "add_subregister"):
                reg1.add_subregister(reg2)

        reg1 = Testy_Register(SAVES_DIR, "hello")
        reg2 = Testy_Register(SAVES_DIR, "hello")
        reg3 = Testy_Register(SAVES_DIR, "hello")
        with reg2.open(): pass
        with reg1.open() as reg1:
            try:
                reg1.add_subregister(reg2)
            except Register_Error:
                self.fail()

        with reg3.open(): pass

        with self.assertRaisesRegex(Register_Error, "read-write"):
            with reg2.open(read_only = True) as reg2:
                reg2.add_subregister(reg3)

        with reg2.open() as reg2:
            try:
                reg2.add_subregister(reg3)
            except Register_Error:
                self.fail()
        with reg1.open() as reg1:
            try:
                reg1.add_subregister(reg3)
            except Register_Error:
                self.fail()

        reg1 = Testy_Register(SAVES_DIR, "hello")
        reg2 = Testy_Register(SAVES_DIR, "hello")
        reg3 = Testy_Register(SAVES_DIR, "hello")
        with reg3.open(): pass
        with reg2.open() as reg2:
            try:
                reg2.add_subregister(reg3)
            except Register_Error:
                self.fail()
        with reg1.open() as reg1:
            try:
                reg1.add_subregister(reg2)
            except Register_Error:
                self.fail()
        with reg3.open() as reg3:
            with self.assertRaises(Register_Error):
                reg3.add_subregister(reg1)

        reg1 = Testy_Register(SAVES_DIR, "hello")
        reg2 = Testy_Register(SAVES_DIR, "hello")

        with reg1.open():pass
        with reg2.open():pass

        with reg1.open() as reg1:

            for debug in [1,2]:

                with self.assertRaises(KeyboardInterrupt):
                        reg1.add_subregister(reg2, debug)

                self.assertEqual(
                    0,
                    lmdb_count_keys(reg1._db, _SUB_KEY_PREFIX)
                )

    def test_remove_subregister(self):

        reg1 = Testy_Register(SAVES_DIR, "hello")
        reg2 = Testy_Register(SAVES_DIR, "hello")
        reg3 = Testy_Register(SAVES_DIR, "hello")

        with reg1.open():pass
        with reg2.open():pass

        with self.assertRaisesRegex(Register_Error, "open.*remove_subregister"):
            reg1.remove_subregister(reg2)

        with reg3.open():pass

        with reg1.open() as reg1:

            reg1.add_subregister(reg2)
            self.assertEqual(
                1,
                lmdb_count_keys(reg1._db, _SUB_KEY_PREFIX)
            )

            reg1.remove_subregister(reg2)
            self.assertEqual(
                0,
                lmdb_count_keys(reg1._db, _SUB_KEY_PREFIX)
            )

            reg1.add_subregister(reg2)
            reg1.add_subregister(reg3)
            self.assertEqual(
                2,
                lmdb_count_keys(reg1._db, _SUB_KEY_PREFIX)
            )

            reg1.remove_subregister(reg2)
            self.assertEqual(
                1,
                lmdb_count_keys(reg1._db, _SUB_KEY_PREFIX)
            )

            for debug in [1,2]:

                with self.assertRaises(KeyboardInterrupt):
                    reg1.remove_subregister(reg3, debug)

                self.assertEqual(
                    1,
                    lmdb_count_keys(reg1._db, _SUB_KEY_PREFIX)
                )

            reg1.remove_subregister(reg3)
            self.assertEqual(
                0,
                lmdb_count_keys(reg1._db, _SUB_KEY_PREFIX)
            )

        with self.assertRaisesRegex(Register_Error, "read-write"):

            with reg1.open(read_only = True) as reg1:
                reg1.remove_subregister(reg2)

    def test_get_all_ram_blocks(self):

        reg = Testy_Register(SAVES_DIR, "whatever")
        apri = Apri_Info(name = "whatev")

        with reg.open() as reg: pass
        with self.assertRaisesRegex(Register_Error, "get_all_ram_blocks"):
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
        except Register_Error:
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
        with self.assertRaisesRegex(Register_Error, "get_ram_block_by_n"):
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
        except Register_Error:
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

    def test_disk_intervals(self):

        reg = Testy_Register(SAVES_DIR, "sup")

        apri1 = Apri_Info(descr = "hello")
        apri2 = Apri_Info(descr = "hey")

        with self.assertRaisesRegex(Register_Error, "open.*disk_intervals"):
            reg.disk_intervals(apri1)

        with reg.open() as reg:

            for apri in [apri1, apri2]:

                with self.assertRaisesRegex(Data_Not_Found_Error, "Apri_Info"):
                    reg.disk_intervals(apri)


        with reg.open() as reg:

            reg.add_disk_block(Block(list(range(50)), apri1))

            self.assertEqual(
                [(0, 50)],
                reg.disk_intervals(apri1)
            )

            with self.assertRaisesRegex(Data_Not_Found_Error, "Apri_Info"):
                reg.disk_intervals(apri2)

            reg.add_disk_block(Block(list(range(100)), apri1))

            self.assertEqual(
                [(0, 100), (0, 50)],
                reg.disk_intervals(apri1)
            )

            reg.add_disk_block(Block(list(range(1000)), apri1, 1))

            self.assertEqual(
                [(0, 100), (0, 50), (1, 1000)],
                reg.disk_intervals(apri1)
            )

            reg.add_disk_block(Block(list(range(420)), apri2, 69))

            self.assertEqual(
                [(0, 100), (0, 50), (1, 1000)],
                reg.disk_intervals(apri1)
            )

            self.assertEqual(
                [(69, 420)],
                reg.disk_intervals(apri2)
            )

        # blk = Block(list(range(50)), )

    def test__iter_ram_and_disk_block_datas(self):pass

    def test_get_disk_block_again(self):

        reg = Numpy_Register(SAVES_DIR, "test")

        apri1 = Apri_Info(descr = "hey")

        with self.assertRaisesRegex(Register_Error, "open.*get_disk_block"):
            reg.get_disk_block(apri1)

        with reg.open() as reg:

            with self.assertRaisesRegex(TypeError, "Apri_Info"):
                reg.get_disk_block("poo")

            with self.assertRaisesRegex(TypeError, "int"):
                reg.get_disk_block(apri1, "butt")

            with self.assertRaisesRegex(TypeError, "int"):
                reg.get_disk_block(apri1, 0, "dumb")

            with self.assertRaisesRegex(ValueError, "non-negative"):
                reg.get_disk_block(apri1, -1)

            with self.assertRaisesRegex(ValueError, "non-negative"):
                reg.get_disk_block(apri1, 0, -1)

            with self.assertRaises(ValueError):
                reg.get_disk_block(apri1, length= -1)

            reg.add_disk_block(Block(list(range(50)), apri1))

            self.assertTrue(np.all(
                reg.get_disk_block(apri1).get_segment() == np.arange(50)
            ))

            self.assertTrue(np.all(
                reg.get_disk_block(apri1, 0).get_segment() == np.arange(50)
            ))

            self.assertTrue(np.all(
                reg.get_disk_block(apri1, 0, 50).get_segment() == np.arange(50)
            ))

            reg.add_disk_block(Block(list(range(51)), apri1))

            self.assertTrue(np.all(
                reg.get_disk_block(apri1).get_segment() == np.arange(51)
            ))

            self.assertTrue(np.all(
                reg.get_disk_block(apri1, 0).get_segment() == np.arange(51)
            ))

            self.assertTrue(np.all(
                reg.get_disk_block(apri1, 0, 51).get_segment() == np.arange(51)
            ))

            self.assertTrue(np.all(
                reg.get_disk_block(apri1, 0, 50).get_segment() == np.arange(50)
            ))

            reg.add_disk_block(Block(list(range(100)), apri1, 1))

            self.assertTrue(np.all(
                reg.get_disk_block(apri1).get_segment() == np.arange(51)
            ))

            self.assertTrue(np.all(
                reg.get_disk_block(apri1, 0).get_segment() == np.arange(51)
            ))

            self.assertTrue(np.all(
                reg.get_disk_block(apri1, 0, 51).get_segment() == np.arange(51)
            ))

            self.assertTrue(np.all(
                reg.get_disk_block(apri1, 0, 50).get_segment() == np.arange(50)
            ))

            self.assertTrue(np.all(
                reg.get_disk_block(apri1, 1, 100).get_segment() == np.arange(100)
            ))

    # def test_get_all_apri_info(self):
    #
    #     reg = Testy_Register(SAVES_DIR, "test")
    #
    #     with self.assertRaisesRegex(Register_Error, "open.*get_all_apri_info"):
    #         reg.get_all_apri_info()
    #
    #     for i in range(200):
    #
    #         apri1 = Apri_Info(name = i)
    #         apri2 = Apri_Info(name = f"{i}")
    #
    #         with reg.open() as reg:
    #
    #             reg.add_disk_block(Block([1], apri1))
    #             reg.add_ram_block(Block([1], apri2))
    #
    #             get = reg.get_all_apri_info()
    #
    #         self.assertEqual(
    #             2*(i+1),
    #             len(get)
    #         )
    #
    #         for j in range(i+1):
    #
    #             self.assertIn(
    #                 Apri_Info(name = i),
    #                 get
    #             )
    #
    #             self.assertIn(
    #                 Apri_Info(name = f"{i}"),
    #                 get
    #             )

    def _is_compressed_helper(self, reg, apri, start_n, length, data_file_bytes = None):

        compressed_key = reg._get_disk_block_key(_COMPRESSED_KEY_PREFIX, apri, None, start_n, length, False)

        self.assertTrue(lmdb_has_key(reg._db, compressed_key))

        with reg._db.begin() as txn:
            val = txn.get(compressed_key)

        self.assertNotEqual(val, _IS_NOT_COMPRESSED_VAL)

        zip_filename = (reg._local_dir / val.decode("ASCII")).with_suffix(".zip")

        self.assertTrue(zip_filename.exists())

        self.assertEqual(zip_filename.suffix, ".zip")

        data_key = reg._get_disk_block_key(_BLK_KEY_PREFIX, apri, None, start_n, length, False)

        self.assertTrue(lmdb_has_key(reg._db, data_key))

        if data_file_bytes is not None:

            with reg._db.begin() as txn:
                self.assertEqual(txn.get(data_key), data_file_bytes)

            data_filename = reg._local_dir / data_file_bytes.decode("ASCII")

            self.assertTrue(data_filename.exists())

            self.assertLessEqual(os.stat(data_filename).st_size, 2)

    def _is_not_compressed_helper(self, reg, apri, start_n, length):

        compressed_key = reg._get_disk_block_key(_COMPRESSED_KEY_PREFIX, apri, None, start_n, length, False)

        self.assertTrue(lmdb_has_key(reg._db, compressed_key))

        with reg._db.begin() as txn:
            self.assertEqual(txn.get(compressed_key), _IS_NOT_COMPRESSED_VAL)

        data_key = reg._get_disk_block_key(_BLK_KEY_PREFIX, apri, None, start_n, length, False)

        with reg._db.begin() as txn:
            return txn.get(data_key)

    def test_compress(self):

        reg2 = Numpy_Register(SAVES_DIR, "testy2")

        with self.assertRaisesRegex(Register_Error, "open.*compress"):
            reg2.compress(Apri_Info(num = 0))

        apri1 = Apri_Info(descr = "sup")
        apri2 = Apri_Info(descr = "hey")
        apris = [apri1, apri1, apri2]

        length1 = 500
        blk1 = Block(np.arange(length1), apri1)
        length2 = 1000000
        blk2 = Block(np.arange(length2), apri1)
        length3 = 2000
        blk3 = Block(np.arange(length3), apri2)
        lengths = [length1, length2, length3]

        with reg2.open() as reg2:
            reg2.add_disk_block(blk1)
            reg2.add_disk_block(blk2)
            reg2.add_disk_block(blk3)

            for i, (apri, length) in enumerate(zip(apris, lengths)):

                data_file_bytes = self._is_not_compressed_helper(reg2, apri, 0, length)
                reg2.compress(apri, 0, length)
                self._is_compressed_helper(reg2, apri, 0, length, data_file_bytes)

                for _apri, _length in zip(apris[i+1:], lengths[i+1:]):

                    self._is_not_compressed_helper(reg2, _apri, 0, _length)

                expected = str(apri).replace("(", "\\(").replace(")", "\\)") + f".*start_n.*0.*length.*{length}"

                with self.assertRaisesRegex(Compression_Error, expected):
                    reg2.compress(apri, 0, length)

        with self.assertRaisesRegex(Register_Error, "read-write"):
            with reg2.open(read_only = True) as reg2:
                reg2.compress(Apri_Info(num = 0))

        reg = Numpy_Register(SAVES_DIR, "no")

        with reg.open() as reg:

            apri = Apri_Info(num = 7)
            blk = Block(np.arange(40), apri)
            reg.add_disk_block(blk)

            for debug in [1,2,3,4]:

                with self.assertRaises(KeyboardInterrupt):
                    reg.compress(apri, debug = debug)

                self._is_not_compressed_helper(reg, apri, 0, 40)

    def test_decompress(self):

        reg1 = Numpy_Register(SAVES_DIR, "lol")

        apri1 = Apri_Info(descr = "LOL")
        apri2 = Apri_Info(decr = "HAHA")
        apris = [apri1, apri1, apri2]

        with self.assertRaisesRegex(Register_Error, "open.*decompress"):
            reg1.decompress(apri1)

        lengths = [50, 500, 5000]
        start_ns = [0, 0, 1000]

        data = [np.arange(length) for length in lengths]

        blks = [Block(*t) for t in zip(data, apris, start_ns)]

        data_files_bytes = []

        with reg1.open() as reg1:

            for blk in blks:
                reg1.add_disk_block(blk)
                data_files_bytes.append(
                    self._is_not_compressed_helper(reg1, blk.get_apri(), blk.get_start_n(), len(blk))
                )

            for t in zip(apris, start_ns, lengths):
                reg1.compress(*t)

            for i, t in enumerate(zip(apris, start_ns, lengths)):

                reg1.decompress(*t)

                self._is_not_compressed_helper(reg1, *t)

                for _t in zip(apris[i+1:], start_ns[i+1:], lengths[i+1:], data_files_bytes[i+1:]):

                    self._is_compressed_helper(reg1, *_t)

                expected = str(t[0]).replace("(", "\\(").replace(")", "\\)") + f".*start_n.*0.*length.*{t[2]}"
                with self.assertRaisesRegex(Decompression_Error, expected):
                    reg1.decompress(*t)

        with self.assertRaisesRegex(Register_Error, "read-only"):

            with reg1.open(read_only = True) as reg1:
                reg1.decompress(apri1)

        reg2 = Numpy_Register(SAVES_DIR, "hi")

        with reg2.open() as reg2:

            apri = Apri_Info(hi = "hello")
            blk1 = Block(np.arange(15), apri)
            blk2 = Block(np.arange(15, 30), apri, 15)

            reg2.add_disk_block(blk1)
            reg2.add_disk_block(blk2)

            reg2.compress(apri, 0, 15)
            reg2.compress(apri, 15, 15)

            for debug in [1, 2, 3, 4]:

                with self.assertRaises(KeyboardInterrupt):
                    reg2.decompress(apri, 15, 15, False, debug)

                with reg2._db.begin() as txn:

                    blk_filename1 = txn.get(reg2._get_disk_block_key(_BLK_KEY_PREFIX, apri, None, 0, 15, False))
                    blk_filename2 = txn.get(reg2._get_disk_block_key(_BLK_KEY_PREFIX, apri, None, 15, 15, False))

                    self._is_compressed_helper(reg2, apri, 0, 15, blk_filename1)
                    self._is_compressed_helper(reg2, apri, 15, 15, blk_filename2)

            reg2.decompress(apri, 0, 15)
            reg2.decompress(apri, 15, 15)

            self._is_not_compressed_helper(reg2, apri, 0, 15)
            self._is_not_compressed_helper(reg2, apri, 15, 15)


        # with reg2.open() as reg2:
        #
        #     reg2.add_disk_block(Block(list(range(10)), apri1))
        #
        #     reg2.compress(apri1)
        #
        #     for key, val in reg2._iter_disk_block_pairs(_COMPRESSED_KEY_PREFIX, apri1, None):
        #
        #         compr_filename = reg2._local_dir / val.decode("ASCII")
        #
        #         with reg2._db.begin() as txn:
        #             data_filename = txn.get(_BLK_KEY_PREFIX + key[ : _COMPRESSED_KEY_PREFIX_LEN])
        #
        #         with compr_filename.open("a"):
        #
        #             with self.assertRaises(OSError):
        #                 reg2.decompress(apri1)
        #
        #         self._is_compressed_helper(reg2, apri1, 0, 10, data_filename)
        #         break
        #
        #     for _, val in reg2._iter_disk_block_pairs(_BLK_KEY_PREFIX, apri1, None):
        #
        #         filename = reg2._local_dir / val.decode("ASCII")
        #
        #         with filename.open("a"):
        #
        #             with self.assertRaises(OSError):
        #                 reg2.decompress(apri1)
        #
        #         self._is_compressed_helper(reg2, apri1, 0, 10, filename.name.encode("ASCII"))
        #         break

    # def test_compress_all(self):
    #
    #     reg = Numpy_Register(SAVES_DIR, "lol")
    #     apri1 = Apri_Info(descr = "Suuuuup")
    #     apri2 = Apri_Info(descr="Suuuuupdfffd")
    #     blk1 = Block(np.arange(10000), apri1)
    #     blk2 = Block(np.arange(1000), apri1)
    #     blk3 = Block(np.arange(30000), apri1, 42069)
    #     blk4 = Block(np.arange(10000), apri2)
    #
    #     with reg.open() as reg:
    #
    #         expected = "`" + str(apri1).replace("(", "\\(").replace(")", "\\)") + "`"
    #         with self.assertRaisesRegex(Data_Not_Found_Error, expected):
    #             reg.compress_all(apri1)
    #
    #         reg.add_disk_block(blk1)
    #
    #         data_file_bytes1 = self._is_not_compressed_helper(reg, apri1, 0, 10000)
    #
    #         reg.compress_all(apri1)
    #
    #         self._is_compressed_helper(reg, apri1, 0, 10000, data_file_bytes1)
    #
    #         reg.add_disk_block(blk2)
    #         data_file_bytes2 = self._is_not_compressed_helper(reg, apri1, 0, 1000)
    #         reg.add_disk_block(blk3)
    #         data_file_bytes3 = self._is_not_compressed_helper(reg, apri1, 42069, 30000)
    #         reg.add_disk_block(blk4)
    #         data_file_bytes4 = self._is_not_compressed_helper(reg, apri2, 0, 10000)
    #
    #         reg.compress_all(apri1)
    #
    #         self._is_compressed_helper(reg, apri1, 0, 10000, data_file_bytes1)
    #         self._is_compressed_helper(reg, apri1, 0, 1000, data_file_bytes2)
    #         self._is_compressed_helper(reg, apri1, 42069, 30000, data_file_bytes3)
    #         self._is_not_compressed_helper(reg, apri2, 0, 10000)
    #
    #         try:
    #             reg.compress_all(apri1)
    #         except RuntimeError:
    #             self.fail()

    # def test_decompress_all(self):
    #
    #     reg = Numpy_Register(SAVES_DIR, "lol")
    #     apri1 = Apri_Info(descr="Suuuuup")
    #     apri2 = Apri_Info(descr="Suuuuupdfffd")
    #     blk1 = Block(np.arange(10000), apri1)
    #     blk2 = Block(np.arange(1000), apri1)
    #     blk3 = Block(np.arange(30000), apri1, 42069)
    #     blk4 = Block(np.arange(10000), apri2)
    #
    #     with reg.open() as reg:
    #
    #         expected = "`" + str(apri1).replace("(", "\\(").replace(")", "\\)") + "`"
    #         with self.assertRaisesRegex(Data_Not_Found_Error, expected):
    #             reg.decompress_all(apri1)
    #
    #         reg.add_disk_block(blk1)
    #         reg.add_disk_block(blk2)
    #
    #         data_file_bytes1 = self._is_not_compressed_helper(reg, apri1, 0, 10000)
    #         data_file_bytes2 = self._is_not_compressed_helper(reg, apri1, 0, 1000)
    #
    #         reg.compress_all(apri1)
    #         reg.decompress_all(apri1)
    #
    #         self._is_not_compressed_helper(reg, apri1, 0, 10000)
    #         self._is_not_compressed_helper(reg, apri1, 0, 1000)
    #
    #         try:
    #             reg.decompress_all(apri1)
    #         except RuntimeError:
    #             self.fail()
    #
    #         reg.add_disk_block(blk3)
    #         reg.add_disk_block(blk4)
    #
    #         data_file_bytes3 = self._is_not_compressed_helper(reg, apri1, 42069, 30000)
    #         data_file_bytes4 = self._is_not_compressed_helper(reg, apri2, 0, 10000)
    #
    #         reg.compress_all(apri1)
    #
    #         self._is_compressed_helper(reg, apri1, 0, 10000, data_file_bytes1)
    #         self._is_compressed_helper(reg, apri1, 0, 1000, data_file_bytes2)
    #         self._is_compressed_helper(reg, apri1, 42069, 30000, data_file_bytes3)
    #
    #         reg.compress(apri2, 0, 10000)
    #
    #         self._is_compressed_helper(reg, apri2, 0, 10000, data_file_bytes4)
    #
    #         reg.decompress_all(apri1)
    #
    #         self._is_not_compressed_helper(reg, apri1, 0, 10000)
    #         self._is_not_compressed_helper(reg, apri1, 0, 1000)
    #         self._is_not_compressed_helper(reg, apri1, 42069, 30000)

    def test_change_apri_info(self):

        reg = Testy_Register(SAVES_DIR, "msg")

        with self.assertRaisesRegex(Register_Error, "open.*change_apri_info"):
            reg.change_apri_info(Apri_Info(i = 0), Apri_Info(j=0))

        with reg.open() as reg:

            old_apri = Apri_Info(sup = "hey")
            new_apri = Apri_Info(hello = "hi")
            apos = Apos_Info(hey = "sup")

            with self.assertRaisesRegex(Data_Not_Found_Error, re.escape(str(old_apri))):
                reg.change_apri_info(old_apri, new_apri)

            reg.set_apos_info(old_apri, apos)

            reg.change_apri_info(old_apri, new_apri)

            with self.assertRaisesRegex(Data_Not_Found_Error, re.escape(str(old_apri))):
                reg.get_apos_info(old_apri)

            self.assertEqual(
                apos,
                reg.get_apos_info(new_apri)
            )

            self.assertEqual(
                1,
                len(reg.get_all_apri_info())
            )

            self.assertEqual(
                1,
                lmdb_count_keys(reg._db, _APRI_ID_KEY_PREFIX)
            )

            self.assertEqual(
                1,
                lmdb_count_keys(reg._db, _ID_APRI_KEY_PREFIX)
            )

            self.assertIn(
                new_apri,
                reg
            )

            self.assertNotIn(
                old_apri,
                reg
            )

        with reg.open(read_only = True) as reg:
            with self.assertRaisesRegex(Register_Error, "read-write"):
                reg.change_apri_info(old_apri, new_apri)

        reg = Numpy_Register(SAVES_DIR, "hello")

        with reg.open() as reg:

            old_apri = Apri_Info(sup = "hey")
            other_apri = Apri_Info(sir = "maam", respective = old_apri)
            new_apri = Apri_Info(hello = "hi")
            new_other_apri = Apri_Info(respective = new_apri, sir = "maam")

            apos1 = Apos_Info(some = "info")
            apos2 = Apos_Info(some_more = "info")

            reg.set_apos_info(old_apri, apos1)
            reg.set_apos_info(other_apri, apos2)

            reg.change_apri_info(old_apri, new_apri)

            with self.assertRaisesRegex(Data_Not_Found_Error, re.escape(str(old_apri))):
                reg.get_apos_info(old_apri)

            with self.assertRaisesRegex(Data_Not_Found_Error, re.escape(str(other_apri))):
                reg.get_apos_info(other_apri)

            self.assertEqual(
                apos1,
                reg.get_apos_info(new_apri)
            )

            self.assertEqual(
                apos2,
                reg.get_apos_info(new_other_apri)
            )

            self.assertIn(
                new_apri,
                reg
            )

            self.assertIn(
                new_other_apri,
                reg
            )

            self.assertNotIn(
                old_apri,
                reg
            )

            self.assertNotIn(
                other_apri,
                reg
            )

            get = reg.get_all_apri_info()

            self.assertEqual(
                2,
                len(get)
            )

            self.assertIn(
                new_apri,
                get
            )

            self.assertIn(
                new_other_apri,
                get
            )

            self.assertEqual(
                2,
                lmdb_count_keys(reg._db, _APRI_ID_KEY_PREFIX)
            )

            self.assertEqual(
                2,
                lmdb_count_keys(reg._db, _ID_APRI_KEY_PREFIX)
            )

            # change it back

            reg.change_apri_info(new_apri, old_apri)

            with self.assertRaisesRegex(Data_Not_Found_Error, re.escape(str(new_apri))):
                reg.get_apos_info(new_apri)

            with self.assertRaisesRegex(Data_Not_Found_Error, re.escape(str(new_other_apri))):
                reg.get_apos_info(new_other_apri)

            self.assertEqual(
                apos1,
                reg.get_apos_info(old_apri)
            )

            self.assertEqual(
                apos2,
                reg.get_apos_info(other_apri)
            )

            self.assertIn(
                old_apri,
                reg
            )

            self.assertIn(
                other_apri,
                reg
            )

            self.assertNotIn(
                new_apri,
                reg
            )

            self.assertNotIn(
                new_other_apri,
                reg
            )

            get = reg.get_all_apri_info()

            self.assertEqual(
                2,
                len(get)
            )

            self.assertIn(
                old_apri,
                get
            )

            self.assertIn(
                other_apri,
                get
            )

            self.assertEqual(
                2,
                lmdb_count_keys(reg._db, _APRI_ID_KEY_PREFIX)
            )

            self.assertEqual(
                2,
                lmdb_count_keys(reg._db, _ID_APRI_KEY_PREFIX)
            )


            # change to an apri that already exists in the register

            blk = Block(np.arange(100), other_apri)
            reg.add_disk_block(blk)

            reg.change_apri_info(old_apri, other_apri)

            other_other_apri = Apri_Info(sir = "maam", respective = other_apri)

            with self.assertRaisesRegex(Data_Not_Found_Error, re.escape(str(old_apri))):
                reg.get_apos_info(old_apri)

            try:
                reg.get_apos_info(other_apri)

            except Data_Not_Found_Error:
                self.fail("It does contain other_apri")

            except Exception as e:
                raise e

            self.assertEqual(
                apos1,
                reg.get_apos_info(other_apri)
            )

            self.assertEqual(
                apos2,
                reg.get_apos_info(other_other_apri)
            )

            self.assertEqual(
                Block(np.arange(100), other_other_apri),
                reg.get_disk_block(other_other_apri)
            )

            self.assertIn(
                other_apri,
                reg
            )

            self.assertIn(
                other_other_apri,
                reg
            )

            self.assertNotIn(
                old_apri,
                reg
            )

            self.assertNotIn(
                new_apri,
                reg
            )

            self.assertNotIn(
                new_other_apri,
                reg
            )

            get = reg.get_all_apri_info()

            self.assertEqual(
                2,
                len(get)
            )

            self.assertIn(
                other_apri,
                get
            )

            self.assertIn(
                other_other_apri,
                get
            )

            self.assertEqual(
                2,
                lmdb_count_keys(reg._db, _APRI_ID_KEY_PREFIX)
            )

            self.assertEqual(
                2,
                lmdb_count_keys(reg._db, _ID_APRI_KEY_PREFIX)
            )

            # change to an apri that creates duplicate keys

            with self.assertRaisesRegex(ValueError, "disjoint"):
                reg.change_apri_info(other_other_apri, other_apri)

        reg = Numpy_Register(SAVES_DIR, "hello")

        with reg.open() as reg:

            apri1 = Apri_Info(hi = "hello")
            apri2 = Apri_Info(num = 7, respective = apri1)

            reg.set_apos_info(apri1, Apos_Info(no = "yes"))
            reg.add_disk_block(Block(np.arange(10), apri2))

            for debug in [1,2,3]:

                with self.assertRaises(KeyboardInterrupt):
                    reg.change_apri_info(apri1, Apri_Info(sup = "hey"), False, debug)

                self.assertEqual(
                    Apos_Info(no = "yes"),
                    reg.get_apos_info(Apri_Info(hi = "hello"))
                )

                self.assertTrue(np.all(
                    np.arange(10) ==
                    reg.get_disk_block(Apri_Info(num = 7, respective = Apri_Info(hi = "hello")), 0, 10).get_segment()
                ))

                self.assertIn(
                    Apri_Info(hi = "hello"),
                    reg
                )

                self.assertIn(
                    Apri_Info(num = 7, respective = Apri_Info(hi = "hello")),
                    reg
                )

                self.assertNotIn(
                    Apri_Info(sup = "hey"),
                    reg
                )

                self.assertNotIn(
                    Apri_Info(num = 7, respective = Apri_Info(sup = "hey")),
                    reg
                )

                get = reg.get_all_apri_info()

                self.assertEqual(
                    2,
                    len(get)
                )

                self.assertIn(
                    Apri_Info(hi = "hello"),
                    get
                )

                self.assertIn(
                    Apri_Info(num = 7, respective = Apri_Info(hi = "hello")),
                    get
                )

                self.assertEqual(
                    2,
                    lmdb_count_keys(reg._db, _APRI_ID_KEY_PREFIX)
                )

                self.assertEqual(
                    2,
                    lmdb_count_keys(reg._db, _ID_APRI_KEY_PREFIX)
                )

    def test_concatenate_disk_blocks(self):

        reg = Numpy_Register(SAVES_DIR, "hello")

        with self.assertRaisesRegex(Register_Error, "open.*concatenate_disk_blocks"):
            reg.concatenate_disk_blocks(Apri_Info(_ = "_"), 0, 0)

        with reg.open() as reg:

            apri = Apri_Info(hi = "hello")

            blk1 = Block(np.arange(100), apri)
            blk2 = Block(np.arange(100, 200), apri, 100)

            reg.add_disk_block(blk1)
            reg.add_disk_block(blk2)

            with self.assertRaisesRegex(ValueError, "too long"):
                reg.concatenate_disk_blocks(apri, 0, 150, True)

            self.assertEqual(
                2,
                lmdb_count_keys(reg._db, _BLK_KEY_PREFIX)
            )

            self.assertEqual(
                2,
                lmdb_count_keys(reg._db, _COMPRESSED_KEY_PREFIX)
            )

            with self.assertRaisesRegex(ValueError, "too long"):
                reg.concatenate_disk_blocks(apri, 1, 200)

            with self.assertRaisesRegex(ValueError, "too long"):
                reg.concatenate_disk_blocks(apri, 0, 199)

            try:
                reg.concatenate_disk_blocks(apri, 0, 200, True)

            except Exception as e:
                self.fail("concatenate_disk_blocks call should have succeeded")

            self.assertEqual(
                1,
                lmdb_count_keys(reg._db, _BLK_KEY_PREFIX)
            )

            self.assertEqual(
                1,
                lmdb_count_keys(reg._db, _COMPRESSED_KEY_PREFIX)
            )

            self.assertEqual(
                Block(np.arange(200), apri),
                reg.get_disk_block(apri, 0, 200)
            )

            self.assertEqual(
                Block(np.arange(200), apri),
                reg.get_disk_block(apri)
            )

            try:
                # this shouldn't do anything
                reg.concatenate_disk_blocks(apri)

            except Exception as e:
                self.fail("combine call should have worked.")

            self.assertEqual(
                1,
                lmdb_count_keys(reg._db, _BLK_KEY_PREFIX)
            )

            self.assertEqual(
                1,
                lmdb_count_keys(reg._db, _COMPRESSED_KEY_PREFIX)
            )

            self.assertEqual(
                Block(np.arange(200), apri),
                reg.get_disk_block(apri, 0, 200)
            )

            self.assertEqual(
                Block(np.arange(200), apri),
                reg.get_disk_block(apri)
            )

            blk3 = Block(np.arange(200, 4000), apri, 200)

            reg.add_disk_block(blk3)

            reg.concatenate_disk_blocks(apri, delete = True)

            self.assertEqual(
                1,
                lmdb_count_keys(reg._db, _BLK_KEY_PREFIX)
            )

            self.assertEqual(
                1,
                lmdb_count_keys(reg._db, _COMPRESSED_KEY_PREFIX)
            )

            self.assertEqual(
                Block(np.arange(4000), apri),
                reg.get_disk_block(apri, 0, 4000)
            )

            self.assertEqual(
                Block(np.arange(4000), apri),
                reg.get_disk_block(apri)
            )

            blk4 = Block(np.arange(4001, 4005), apri, 4001)

            reg.add_disk_block(blk4)

            # this shouldn't do anything
            reg.concatenate_disk_blocks(apri)

            self.assertEqual(
                2,
                lmdb_count_keys(reg._db, _BLK_KEY_PREFIX)
            )

            self.assertEqual(
                2,
                lmdb_count_keys(reg._db, _COMPRESSED_KEY_PREFIX)
            )

            self.assertEqual(
                Block(np.arange(4000), apri),
                reg.get_disk_block(apri, 0, 4000)
            )

            self.assertEqual(
                Block(np.arange(4000), apri),
                reg.get_disk_block(apri)
            )

            with self.assertRaisesRegex(Data_Not_Found_Error, "4000"):
                reg.concatenate_disk_blocks(apri, 0, 4001)

            with self.assertRaisesRegex(Data_Not_Found_Error, "4000.*4000"):
                reg.concatenate_disk_blocks(apri, 0, 4005)

            blk5 = Block(np.arange(3999, 4001), apri, 3999)

            reg.add_disk_block(blk5)

            with self.assertRaisesRegex(ValueError, "[oO]verlap"):
                reg.concatenate_disk_blocks(apri, 0, 4001)

            blk6 = Block(np.arange(4005, 4100), apri, 4005)
            blk7 = Block(np.arange(4100, 4200), apri, 4100)
            blk8 = Block(np.arange(4200, 4201), apri, 4200)

            reg.add_disk_block(blk6)
            reg.add_disk_block(blk7)
            reg.add_disk_block(blk8)

            reg.concatenate_disk_blocks(apri, 4005, delete = True)

            self.assertEqual(
                4,
                lmdb_count_keys(reg._db, _BLK_KEY_PREFIX)
            )

            self.assertEqual(
                4,
                lmdb_count_keys(reg._db, _COMPRESSED_KEY_PREFIX)
            )

            self.assertEqual(
                Block(np.arange(4005, 4201), apri, 4005),
                reg.get_disk_block(apri, 4005, 4201 - 4005)
            )

            self.assertEqual(
                Block(np.arange(4005, 4201), apri, 4005),
                reg.get_disk_block(apri, 4005)
            )

            self.assertEqual(
                Block(np.arange(4000), apri),
                reg.get_disk_block(apri)
            )

            blk9 = Block(np.arange(4201, 4201), apri, 4201)
            reg.add_disk_block(blk9)

            reg.concatenate_disk_blocks(apri, 4005, delete = True)

            self.assertEqual(
                5,
                lmdb_count_keys(reg._db, _BLK_KEY_PREFIX)
            )

            self.assertEqual(
                5,
                lmdb_count_keys(reg._db, _COMPRESSED_KEY_PREFIX)
            )

            self.assertEqual(
                Block(np.arange(4005, 4201), apri, 4005),
                reg.get_disk_block(apri, 4005, 4201 - 4005)
            )

            self.assertEqual(
                Block(np.arange(4005, 4201), apri, 4005),
                reg.get_disk_block(apri, 4005)
            )

            self.assertEqual(
                Block(np.arange(4000), apri),
                reg.get_disk_block(apri)
            )

            self.assertEqual(
                Block(np.arange(4201, 4201), apri, 4201),
                reg.get_disk_block(apri, 4201, 0)
            )

            blk10 = Block(np.arange(0, 0), apri, 0)
            reg.add_disk_block(blk10)

            reg.remove_disk_block(apri, 3999, 2)

            reg.concatenate_disk_blocks(apri, delete = True)

            self.assertEqual(
                5,
                lmdb_count_keys(reg._db, _BLK_KEY_PREFIX)
            )

            self.assertEqual(
                5,
                lmdb_count_keys(reg._db, _COMPRESSED_KEY_PREFIX)
            )

            self.assertEqual(
                Block(np.arange(4005, 4201), apri, 4005),
                reg.get_disk_block(apri, 4005, 4201 - 4005)
            )

            self.assertEqual(
                Block(np.arange(4005, 4201), apri, 4005),
                reg.get_disk_block(apri, 4005)
            )

            self.assertEqual(
                Block(np.arange(4000), apri),
                reg.get_disk_block(apri)
            )

            self.assertEqual(
                Block(np.arange(4201, 4201), apri, 4201),
                reg.get_disk_block(apri, 4201, 0)
            )

            self.assertEqual(
                Block(np.arange(0, 0), apri, 0),
                reg.get_disk_block(apri, 0, 0)
            )


        with reg.open(read_only = True) as reg:
            with self.assertRaisesRegex(Register_Error, "[rR]ead-write"):
                reg.concatenate_disk_blocks(Apri_Info(_="_"), 0, 0)

    def _composite_helper(self, reg, block_datas, apris):

        with reg._db.begin() as txn:

            # check blocks
            for data, (seg, compressed) in block_datas.items():

                filename = (txn
                            .get(reg._get_disk_block_key(_BLK_KEY_PREFIX, data[0], None, data[1], data[2], False))
                            .decode("ASCII")
                )

                filename = reg._local_dir / filename

                self.assertTrue(filename.is_file())

                val = txn.get(reg._get_disk_block_key(_COMPRESSED_KEY_PREFIX, data[0], None, data[1], data[2], False))

                self.assertEqual(
                    compressed,
                    val != _IS_NOT_COMPRESSED_VAL
                )

                if val == _IS_NOT_COMPRESSED_VAL:

                    self.assertEqual(
                        Block(seg, *data[:2]),
                        reg.get_disk_block(*data)
                    )

                else:

                    with self.assertRaises(Compression_Error):
                        reg.get_disk_block(*data)

                    filename = reg._local_dir / val.decode("ASCII")

                    self.assertTrue(filename.is_file())

            self.assertEqual(
                len(block_datas),
                lmdb_count_keys(txn, _BLK_KEY_PREFIX)
            )

            self.assertEqual(
                len(block_datas),
                lmdb_count_keys(txn, _COMPRESSED_KEY_PREFIX)
            )

        for apri in apris:

            self.assertEqual(
                sum(_apri == apri for _apri,_,_ in block_datas),
                reg.get_num_disk_blocks(apri)
            )


        # check apri
        all_apri = reg.get_all_apri_info()

        for apri in apris:

            self.assertIn(
                apri, reg
            )

            self.assertIn(
                apri, all_apri
            )

        self.assertEqual(
            len(apris),
            len(all_apri)
        )

        # check files

        count_files = 0

        for fp in reg._local_dir.iterdir():

            if fp.is_dir():

                self.assertEqual(
                    reg._local_dir / REGISTER_FILENAME,
                    fp
                )

            elif fp.is_file():
                count_files += 1

            else:
                self.fail()

        self.assertEqual(
            len(block_datas) + sum(compressed for _, compressed in block_datas.values()),
            count_files
        )

    def test_composite(self):

        # add data to disk
        # compress it
        # remove some data
        # decompress it
        # get disk data
        # set message
        # remove some data
        # combine disk blocks
        # compress it
        # set_start_n_info
        # increase register size
        # move Register to a different saves_directory
        # change apri info
        # compress one at a time
        # decompress half
        # combine disk blocks
        # increase register size
        # change apri info back

        block_datas = {}
        apris = []

        reg = Numpy_Register(SAVES_DIR, "hello")

        with reg.open() as reg:

            inner_apri = Apri_Info(descr =  "\\\\hello", num = 7)
            apri = Apri_Info(descr = "\\'hi\"", respective = inner_apri)
            apris.append(inner_apri)
            apris.append(apri)
            seg = np.arange(69, 420)
            blk = Block(seg, apri, 1337)
            reg.add_disk_block(blk)
            block_datas[data(blk)] = [seg, False]

            self._composite_helper(reg, block_datas, apris)

            seg = np.arange(69, 69)
            blk = Block(seg, apri, 1337)
            reg.add_disk_block(blk)
            block_datas[data(blk)] = [seg, False]

            self._composite_helper(reg, block_datas, apris)

            apri = Apri_Info(descr = "Apri_Info.from_json(hi = \"lol\")", respective = inner_apri)
            apris.append(apri)
            seg = np.arange(69., 420.)
            blk = Block(seg, apri, 1337)
            reg.add_disk_block(blk)
            block_datas[data(blk)] = [seg, False]

            self._composite_helper(reg, block_datas, apris)

            for start_n, length in reg.disk_intervals(Apri_Info(descr = "Apri_Info.from_json(hi = \"lol\")", respective = inner_apri)):
                reg.compress(Apri_Info(descr = "Apri_Info.from_json(hi = \"lol\")", respective = inner_apri), start_n, length)

            _set_block_datas_compressed(block_datas,
                Apri_Info(descr = "Apri_Info.from_json(hi = \"lol\")", respective = inner_apri)
            )

            self._composite_helper(reg, block_datas, apris)

            for start_n, length in reg.disk_intervals(Apri_Info(descr = "\\'hi\"", respective = inner_apri)):
                reg.compress(Apri_Info(descr = "\\'hi\"", respective = inner_apri), start_n, length)

            _set_block_datas_compressed(block_datas,
                Apri_Info(descr = "\\'hi\"", respective = inner_apri)
            )

            self._composite_helper(reg, block_datas, apris)

            reg.remove_disk_block(
                Apri_Info(descr="\\'hi\"", respective=inner_apri)
            )

            del block_datas[Apri_Info(descr="\\'hi\"", respective=inner_apri), 1337, 420 - 69]

            self._composite_helper(reg, block_datas, apris)

            with self.assertRaisesRegex(ValueError, "`Block`"):
                reg.remove_apri_info(Apri_Info(descr="\\'hi\"", respective=inner_apri))

            reg.remove_disk_block(
                Apri_Info(descr="\\'hi\"", respective=inner_apri)
            )

            del block_datas[Apri_Info(descr="\\'hi\"", respective=inner_apri), 1337, 0]

            self._composite_helper(reg, block_datas, apris)

            reg.remove_apri_info(Apri_Info(descr="\\'hi\"", respective=inner_apri))

            del apris[apris.index(Apri_Info(descr="\\'hi\"", respective=inner_apri))]

            self._composite_helper(reg, block_datas, apris)

            with self.assertRaises(ValueError):
                reg.remove_apri_info(inner_apri)

            reg.decompress(
                Apri_Info(descr = "Apri_Info.from_json(hi = \"lol\")", respective = inner_apri),
                1337,
                420 - 69
            )

            _set_block_datas_compressed(
                block_datas,
                Apri_Info(descr = "Apri_Info.from_json(hi = \"lol\")", respective = inner_apri),
                compressed = False
            )

            self._composite_helper(reg, block_datas, apris)

            new_message = "\\\\new message\"\"\\'"
            reg.set_message(new_message)

            self.assertEqual(
                new_message,
                str(reg)
            )

        self.assertEqual(
            new_message,
            str(reg)
        )

        with reg.open() as reg:

            inner_inner_apri = Apri_Info(inner_apri = inner_apri)
            apri = Apri_Info(inner_apri = inner_inner_apri, love = "Apos_Info(num = 6)")
            apris.append(apri)
            apris.append(inner_inner_apri)

            datas = [(10, 34), (10 + 34, 8832), (10 + 34 + 8832, 0), (10 + 34 + 8832, 54), (10 + 34 + 8832 + 54, 0)]

            for start_n, length in datas:

                seg = np.arange(length, 2 * length)
                blk = Block(seg, apri, start_n)
                reg.add_disk_block(blk)
                block_datas[data(blk)] = [seg, False]

                self._composite_helper(reg, block_datas, apris)

            with self.assertRaisesRegex(ValueError, re.escape(str(apri))):
                reg.remove_apri_info(inner_inner_apri)

            reg.concatenate_disk_blocks(apri, delete = True)

            for _data in datas:
                if _data[1] != 0:
                    del block_datas[(apri,) + _data]

            block_datas[(apri, datas[0][0], sum(length for _, length in datas))] = [
                np.concatenate([np.arange(length, 2*length) for _, length in datas]),
                False
            ]

            self._composite_helper(reg, block_datas, apris)

            reg.concatenate_disk_blocks(apri, delete = True)

            self._composite_helper(reg, block_datas, apris)

            reg.compress(apri)

            block_datas[(apri, datas[0][0], sum(length for _, length in datas))][1] = True

            self._composite_helper(reg, block_datas, apris)

            for apri in reg:

                for start_n, length in reg.disk_intervals(apri):
                    reg.remove_disk_block(apri, start_n, length)

            block_datas = {}

            self._composite_helper(reg, block_datas, apris)

            reg.set_start_n_info(10 ** 13, 4)

            start_n = 10 ** 17

            for i in range(5):

                apri = Apri_Info(longg = "boi")
                blk = Block(np.arange(start_n + i*1000, start_n + (i+1)*1000, dtype = np.int64), apri, start_n + i*1000)
                reg.add_disk_block(blk)

            with self.assertRaisesRegex(IndexError, "head"):
                reg.add_disk_block(Block([], apri))

            for start_n, length in reg.disk_intervals(apri):
                reg.remove_disk_block(apri, start_n, length)

            reg.set_start_n_info()

            reg.increase_register_size(reg.get_register_size() + 1)

            with self.assertRaises(ValueError):
                reg.increase_register_size(reg.get_register_size() - 1)

    def test_remove_apri_info(self):

        reg = Numpy_Register(SAVES_DIR, "sup")

        with self.assertRaisesRegex(Register_Error, "open.*remove_apri_info"):
            reg.remove_apri_info(Apri_Info(no = "yes"))

        with reg.open() as reg:

            apri1 = Apri_Info(hello = "hi")
            apri2 = Apri_Info(sup = "hey")
            apri3 = Apri_Info(respective = apri1)

            reg.add_disk_block(Block(np.arange(15), apri1))
            reg.set_apos_info(apri2, Apos_Info(num = 7))
            reg.add_disk_block(Block(np.arange(15, 30), apri3, 15))

            for i in [1,2,3]:

                apri = eval(f"apri{i}")

                with self.assertRaises(ValueError):
                    reg.remove_apri_info(apri)

                get = reg.get_all_apri_info()

                self.assertEqual(
                    3,
                    len(get)
                )

                self.assertEqual(
                    3,
                    lmdb_count_keys(reg._db, _APRI_ID_KEY_PREFIX)
                )

                self.assertEqual(
                    3,
                    lmdb_count_keys(reg._db, _ID_APRI_KEY_PREFIX)
                )

                for j in [1,2,3]:

                    _apri = eval(f"apri{j}")

                    self.assertIn(
                        _apri,
                        reg
                    )

                    self.assertIn(
                        _apri,
                        get
                    )

            reg.remove_disk_block(apri1, 0, 15)

            for i in [1,2,3]:

                apri = eval(f"apri{i}")

                with self.assertRaises(ValueError):
                    reg.remove_apri_info(apri)

                get = reg.get_all_apri_info()

                self.assertEqual(
                    3,
                    len(get)
                )

                self.assertEqual(
                    3,
                    lmdb_count_keys(reg._db, _APRI_ID_KEY_PREFIX)
                )

                self.assertEqual(
                    3,
                    lmdb_count_keys(reg._db, _ID_APRI_KEY_PREFIX)
                )

                for j in [1, 2, 3]:
                    _apri = eval(f"apri{j}")

                    self.assertIn(
                        _apri,
                        reg
                    )

                    self.assertIn(
                        _apri,
                        get
                    )

            reg.remove_apos_info(apri2)

            for debug in [1,2,3,4]:

                with self.assertRaises(KeyboardInterrupt):
                    reg.remove_apri_info(apri2, debug)

                for i in [1, 3]:

                    apri = eval(f"apri{i}")

                    with self.assertRaises(ValueError):
                        reg.remove_apri_info(apri)

                    get = reg.get_all_apri_info()

                    self.assertEqual(
                        3,
                        len(get)
                    )

                    self.assertEqual(
                        3,
                        lmdb_count_keys(reg._db, _APRI_ID_KEY_PREFIX)
                    )

                    self.assertEqual(
                        3,
                        lmdb_count_keys(reg._db, _ID_APRI_KEY_PREFIX)
                    )

                    for j in [1, 2, 3]:
                        _apri = eval(f"apri{j}")

                        self.assertIn(
                            _apri,
                            reg
                        )

                        self.assertIn(
                            _apri,
                            get
                        )

            reg.remove_apri_info(apri2)

            for i in [1,3]:

                apri = eval(f"apri{i}")

                with self.assertRaises(ValueError):
                    reg.remove_apri_info(apri)

                get = reg.get_all_apri_info()

                self.assertEqual(
                    2,
                    len(get)
                )

                self.assertEqual(
                    2,
                    lmdb_count_keys(reg._db, _APRI_ID_KEY_PREFIX)
                )

                self.assertEqual(
                    2,
                    lmdb_count_keys(reg._db, _ID_APRI_KEY_PREFIX)
                )

                for j in [1, 3]:
                    _apri = eval(f"apri{j}")

                    self.assertIn(
                        _apri,
                        reg
                    )

                    self.assertIn(
                        _apri,
                        get
                    )

            self.assertNotIn(
                apri2,
                reg
            )

            reg.remove_disk_block(apri3, 15, 15)

            reg.remove_apri_info(apri3)

            get = reg.get_all_apri_info()

            self.assertEqual(
                1,
                len(get)
            )

            self.assertEqual(
                1,
                lmdb_count_keys(reg._db, _APRI_ID_KEY_PREFIX)
            )

            self.assertEqual(
                1,
                lmdb_count_keys(reg._db, _ID_APRI_KEY_PREFIX)
            )

            self.assertIn(
                apri1,
                get
            )

            self.assertIn(
                apri1,
                reg
            )

            self.assertNotIn(
                apri2,
                reg
            )

            self.assertNotIn(
                apri3,
                reg
            )

            reg.remove_apri_info(apri1)

            self.assertEqual(
                0,
                len(reg.get_all_apri_info())
            )

            self.assertNotIn(
                apri1,
                reg
            )

            self.assertNotIn(
                apri2,
                reg
            )

            self.assertNotIn(
                apri3,
                reg
            )

        with self.assertRaisesRegex(Register_Error, "read-write"):
            with reg.open(read_only = True) as reg:
                reg.remove_apri_info(Apri_Info(no = "yes"))

def _set_block_datas_compressed(block_datas, apri, start_n = None, length = None, compressed = True):

    for (_apri, _start_n, _length), val in block_datas.items():

        if _apri == apri and (start_n is None or _start_n == start_n) and (length is None or _length == length):

            val[1] = compressed