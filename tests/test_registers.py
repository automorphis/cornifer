import os
import re
import shutil
from itertools import product, chain
from pathlib import Path
from unittest import TestCase

import cornifer
import numpy as np

from cornifer import NumpyRegister, Register, Block, load
from cornifer.info import ApriInfo, AposInfo
from cornifer._utilities import random_unique_filename
from cornifer.errors import RegisterAlreadyOpenError, DataNotFoundError, RegisterError, CompressionError, \
    DecompressionError, RegisterRecoveryError
from cornifer.regfilestructure import REG_FILENAME, VERSION_FILEPATH, MSG_FILEPATH, CLS_FILEPATH, \
    DATABASE_FILEPATH, MAP_SIZE_FILEPATH
from cornifer.registers import _BLK_KEY_PREFIX, _KEY_SEP, \
    _APRI_ID_KEY_PREFIX, _ID_APRI_KEY_PREFIX, _START_N_HEAD_KEY, _START_N_TAIL_LENGTH_KEY, _SUB_KEY_PREFIX, \
    _COMPRESSED_KEY_PREFIX, _IS_NOT_COMPRESSED_VAL, _BLK_KEY_PREFIX_LEN, _SUB_VAL, _APOS_KEY_PREFIX, _NO_DEBUG, \
    _START_N_TAIL_LENGTH_DEFAULT, _LENGTH_LENGTH_KEY, _LENGTH_LENGTH_DEFAULT, _CURR_ID_KEY
from cornifer._utilities.lmdb import lmdb_has_key, lmdb_prefix_iter, lmdb_count_keys, open_lmdb
from cornifer.version import CURRENT_VERSION

"""
PUBLIC READ-WRITE METHODS FOR LMDB:
 - set_startn_info
 - open
 - change_apri
 - rmv_apri
 - set_apos
 - rmv_apos
 - add_subreg
 - rmv_subreg
 - add_disk_blk
 - rmv_disk_blk
 - compress
 - decompress
 - Numpy_Register.concat_disk_blks
 
PROTECTED READ-WRITE METHODS FOR LMDB:
 - _get_id_by_apri
 - _get_apos_key
 - _get_disk_blk_key

"""

"""
- LEVEL 0
    - __init__
    - addSubclass
    - _split_disk_block_key
    - _join_disk_block_data

- LEVEL 1
    - __str__
    - __repr__
    - _check_open_raise (uncreated)
    - _set_local_dir
    - __hash__ (uncreated)
    - __eq__ (uncreated)
    - add_ram_blk

- LEVEL 2
    - open (uncreated)
    - rmv_ram_blk
    - _ramBlkByN (no recursive)
    - ramBlks (no recursive)
    - _iter_ram_block_metadatas 

- LEVEL 3
    - __hash__ (created)
    - __eq__ (created)
    - _check_open_raise (created)
    - _get_id_by_apri (new info)
    
- LEVEL 4
    - _get_instance
    - set_msg
    - add_disk_blk
    - _get_apri_json_by_id
    - apris (no recursive)
    
- LEVEL 5
    - _from_name (same register)
    - _open_created
    - _get_id_by_apri
    - _convert_disk_block_key (no head)
    - set_startn_info

- LEVEL 6
    - _iter_disk_block_metadatas
    - _from_name (different registers)
    - open

- LEVEL 7
    - _recursive_open
    - get_disk_block_by_metadata (no recursive)
    - rmv_disk_blk
    - blks

- LEVEL 8
    - _iter_subregs
    - _diskBlkByN
    
- LEVEL 9
    - _check_no_cycles_from
    - add_subreg
    
- LEVEL 10
    - rmv_subreg
    
"""

SAVES_DIR = random_unique_filename(Path.home())
# SAVES_DIR = Path.home() / "tmp" / "tests"

class Testy_Register(Register):

    @classmethod
    def with_suffix(cls, filename):
        return filename

    @classmethod
    def dump_disk_data(cls, data, filename, **kwargs):
        filename.touch()

    @classmethod
    def load_disk_data(cls, filename, **kwargs):
        return None

    @classmethod
    def clean_disk_data(cls, filename, **kwargs):

        filename = Path(filename)

        try:
            filename.unlink(missing_ok = False)

        except RegisterError:pass

class Testy_Register2(Register):

    @classmethod
    def with_suffix(cls, filename):
        return filename

    @classmethod
    def dump_disk_data(cls, data, filename, **kwargs): pass

    @classmethod
    def load_disk_data(cls, filename, **kwargs): pass

    @classmethod
    def clean_disk_data(cls, filename, **kwargs):pass

def data(blk):
    return blk.apri(), blk.startn(), len(blk)

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
            Testy_Register(SAVES_DIR, "tests")

        SAVES_DIR.mkdir()

        with self.assertRaises(TypeError):
            Testy_Register(SAVES_DIR, 0)

        with self.assertRaises(TypeError):
            Testy_Register(0, "sup")

        self.assertFalse(Testy_Register(SAVES_DIR, "sup")._created)

        self.assertEqual(Testy_Register(SAVES_DIR, "sup")._version, CURRENT_VERSION)

    # def test_add_subclass(self):
    #
    #     with self.assertRaisesRegex(TypeError, "must be a class"):
    #         Register.add_subclass(0)
    #
    #     class Hello:pass
    #
    #     with self.assertRaisesRegex(TypeError, "subclass of `Register`"):
    #         Register.add_subclass(Hello)
    #
    #     Register.add_subclass(Testy_Register2)
    #
    #     self.assertIn(
    #         "Testy_Register2",
    #         Register._constructors.keys()
    #     )
    #
    #     self.assertEqual(
    #         Register._constructors["Testy_Register2"],
    #         Testy_Register2
    #     )

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
                Register._join_disk_block_data(*((_BLK_KEY_PREFIX,) + Register._split_disk_block_key(_BLK_KEY_PREFIX_LEN, key)))
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

        with self.assertRaisesRegex(RegisterError, "tests"):
            reg._check_open_raise("tests")

    def test__set_local_dir(self):

        # tests that error is raised when `local_dir` is not a sub-dir of `savesDir`
        local_dir = SAVES_DIR / "bad" / "test_local_dir"
        reg = Testy_Register(SAVES_DIR, "sup")
        with self.assertRaisesRegex(ValueError, "sub-directory"):
            reg._set_local_dir(local_dir)

        # tests that error is raised when `Register` has not been created
        local_dir = SAVES_DIR / "test_local_dir"
        reg = Testy_Register(SAVES_DIR, "sup")
        with self.assertRaisesRegex(FileNotFoundError, "database"):
            reg._set_local_dir(local_dir)

        # tests that newly created register has the correct filestructure and instance attributes
        # register database must be manually created for this tests case
        local_dir = SAVES_DIR / "test_local_dir"
        reg = Testy_Register(SAVES_DIR, "sup")
        local_dir.mkdir()
        (local_dir / REG_FILENAME).mkdir(exist_ok = False)
        (local_dir / VERSION_FILEPATH).touch(exist_ok = False)
        (local_dir / MSG_FILEPATH).touch(exist_ok = False)
        (local_dir / CLS_FILEPATH).touch(exist_ok = False)
        (local_dir / DATABASE_FILEPATH).mkdir(exist_ok = False)
        (local_dir / MAP_SIZE_FILEPATH).touch(exist_ok = False)

        try:
            reg._db = open_lmdb(local_dir / REG_FILENAME, 1, False)

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
        with self.assertRaisesRegex(RegisterError, "__hash__"):
            hash(Testy_Register(SAVES_DIR, "hey"))

    def test___eq___uncreated(self):
        with self.assertRaises(RegisterError):
            Testy_Register(SAVES_DIR, "hey") == Testy_Register(SAVES_DIR, "sup")

    def test_add_ram_block(self):

        reg = Testy_Register(SAVES_DIR, "msg")
        blk = Block([], ApriInfo(name ="tests"))

        reg = Testy_Register(SAVES_DIR, "msg")
        blk1 = Block([], ApriInfo(name ="tests"))

        with reg.open() as reg:
            reg.add_ram_blk(blk1)

        self.assertEqual(
            1,
            len(reg._ram_blks)
        )

        self.assertEqual(
            1,
            len(reg._ram_blks[ApriInfo(name ="tests")])
        )

        blk2 = Block([], ApriInfo(name = "testy"))

        with reg.open() as reg:
            reg.add_ram_blk(blk2)

        self.assertEqual(
            2,
            len(reg._ram_blks)
        )

        self.assertEqual(
            1,
            len(reg._ram_blks[ApriInfo(name ="tests")])
        )

        self.assertEqual(
            1,
            len(reg._ram_blks[ApriInfo(name ="testy")])
        )

        blk3 = Block([], ApriInfo(name = "testy"))

        with reg.open() as reg:
            reg.add_ram_blk(blk3)

        self.assertEqual(
            2,
            len(reg._ram_blks)
        )

        self.assertEqual(
            1,
            len(reg._ram_blks[ApriInfo(name="tests")])
        )

        self.assertEqual(
            2,
            len(reg._ram_blks[ApriInfo(name="testy")])
        )

        blk4 = Block([1], ApriInfo(name ="testy"))

        with reg.open() as reg:
            reg.add_ram_blk(blk4)

        self.assertEqual(
            2,
            len(reg._ram_blks)
        )

        self.assertEqual(
            1,
            len(reg._ram_blks[ApriInfo(name="tests")])
        )

        self.assertEqual(
            3,
            len(reg._ram_blks[ApriInfo(name="testy")])
        )

    def test_open_uncreated(self):

        reg = Testy_Register(SAVES_DIR, "hey")

        with reg.open() as reg:
            self.assertFalse(reg._db_is_closed())

        self.assertTrue(reg._created)

        keyvals = {
            _START_N_HEAD_KEY : b"0",
            _START_N_TAIL_LENGTH_KEY : str(_START_N_TAIL_LENGTH_DEFAULT).encode("ASCII"),
            _LENGTH_LENGTH_KEY : str(_LENGTH_LENGTH_DEFAULT).encode("ASCII"),
            _CURR_ID_KEY : b"0",
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

        reg = NumpyRegister(SAVES_DIR, "msg")
        blk1 = Block([], ApriInfo(name ="name1"))

        with reg.open() as reg:

            reg.add_ram_blk(blk1)
            reg.rmv_ram_blk(blk1)
            self.assertEqual(
                0,
                len(reg._ram_blks)
            )

            reg.add_ram_blk(blk1)
            reg.rmv_ram_blk(blk1)
            self.assertEqual(
                0,
                len(reg._ram_blks)
            )

            reg.add_ram_blk(blk1)
            blk2 = Block([], ApriInfo(name ="name2"))
            reg.add_ram_blk(blk2)
            reg.rmv_ram_blk(blk1)
            self.assertEqual(
                1,
                len(reg._ram_blks)
            )
            self.assertIs(
                blk2,
                reg._ram_blks[blk2.apri()][0]
            )

            reg.rmv_ram_blk(blk2)
            self.assertEqual(
                0,
                len(reg._ram_blks)
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

        # manually change the `_localDir` to force equality
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

        # manually change the `_localDir` to force equality
        reg2 = Testy_Register(SAVES_DIR, "msg")
        reg2._set_local_dir(reg1._local_dir)
        self.assertEqual(
            reg2,
            reg1
        )

        # tests a different `Register` derived type
        reg2 = Testy_Register2(SAVES_DIR, "msg")
        reg2._set_local_dir(reg1._local_dir)
        self.assertNotEqual(
            reg2,
            reg1
        )

        # tests that relative paths work as expected
        reg2 = Testy_Register(SAVES_DIR, "msg")
        reg2._set_local_dir(".." / SAVES_DIR / reg1._local_dir)
        self.assertEqual(
            reg2,
            reg1
        )

    def test__check_open_raise_created(self):

        reg = Testy_Register(SAVES_DIR, "hi")
        with self.assertRaisesRegex(RegisterError, "xyz"):
            reg._check_open_raise("xyz")

        reg = Testy_Register(SAVES_DIR, "hi")
        with reg.open() as reg:
            try:
                reg._check_open_raise("xyz")
            except RegisterError:
                self.fail("the register is open")

        reg = Testy_Register(SAVES_DIR, "hi")
        with reg.open() as reg:pass
        with self.assertRaisesRegex(RegisterError, "xyz"):
            reg._check_open_raise("xyz")

    def test__get_id_by_apri_new(self):

        reg = Testy_Register(SAVES_DIR, "hi")

        with self.assertRaises(ValueError):
            reg._get_id_by_apri(None, None, True)

        with self.assertRaises(ValueError):
            reg._get_id_by_apri(None, None, False)

        apri1 = ApriInfo(name ="hi")
        apri2 = ApriInfo(name ="hello")
        apri3 = ApriInfo(name ="sup")
        apri4 = ApriInfo(name ="hey")
        reg = Testy_Register(SAVES_DIR, "hi")

        with reg.open() as reg:


            _id1 = reg._get_id_by_apri(apri1, None, True)

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

            with self.assertRaises(DataNotFoundError):
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
            reg.set_msg("yes")

        except RegisterError as e:
            if "has not been opened" in str(e):
                self.fail("the register doesn't need to be open for set_msg")
            else:
                raise e

        self.assertEqual(
            "yes",
            str(reg)
        )

        with reg.open() as reg:pass

        reg.set_msg("no")

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
        blk = Block([], ApriInfo(name ="hi"))
        with self.assertRaisesRegex(RegisterError, "open.*add_disk_blk"):
            reg.add_disk_blk(blk)

        reg = Testy_Register(SAVES_DIR, "hello")
        blk = Block([], ApriInfo(name ="hi"), 10 ** 50)
        with reg.open() as reg:
            with self.assertRaisesRegex(IndexError, "correct head"):
                reg.add_disk_blk(blk)

        reg = Testy_Register(SAVES_DIR, "hello")
        too_large = reg._startn_tail_mod
        blk = Block([], ApriInfo(name ="hi"), too_large)
        with reg.open() as reg:
            with self.assertRaisesRegex(IndexError, "correct head"):
                reg.add_disk_blk(blk)

        reg = Testy_Register(SAVES_DIR, "hello")
        too_large = reg._startn_tail_mod
        blk = Block([], ApriInfo(name ="hi"), too_large - 1)
        with reg.open() as reg:
            try:
                reg.add_disk_blk(blk)
            except IndexError:
                self.fail("index is not too large")

        reg = Testy_Register(SAVES_DIR, "hi")
        blk1 = Block([], ApriInfo(name ="hello"))
        blk2 = Block([1], ApriInfo(name ="hello"))
        blk3 = Block([], ApriInfo(name ="hi"))
        blk4 = Block([], ApriInfo(name ="hello"))
        blk5 = Block([], ApriInfo(sir ="hey", maam ="hi"))
        blk6 = Block([], ApriInfo(maam="hi", sir ="hey"))
        with reg.open() as reg:

            reg.add_disk_blk(blk1)
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

            reg.add_disk_blk(blk2)
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

            reg.add_disk_blk(blk3)
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

            try:
                with self.assertRaisesRegex(RegisterError, "[dD]uplicate"):
                    reg.add_disk_blk(blk4)

            except AssertionError:
                raise

            reg.add_disk_blk(blk5)

            with self.assertRaisesRegex(RegisterError, "[dD]uplicate"):
                reg.add_disk_blk(blk6)

        with self.assertRaisesRegex(RegisterError, "read-only"):
            with reg.open(readonly= True) as reg:
                reg.add_disk_blk(blk)

        reg = NumpyRegister(SAVES_DIR, "no")

        with reg.open() as reg:

            reg.add_disk_blk(Block(np.arange(30), ApriInfo(maybe ="maybe")))

            for debug in [1,2,3,4,5,6,7,8,9,10]:

                apri = ApriInfo(none ="all")
                blk = Block(np.arange(14), apri, 0)

                cornifer.registers._debug = debug

                with self.assertRaises(KeyboardInterrupt):
                    reg.add_disk_blk(blk)

                cornifer.registers._debug = _NO_DEBUG

                try:
                    self.assertEqual(
                        1,
                        lmdb_count_keys(reg._db, _BLK_KEY_PREFIX)
                    )
                except AssertionError:
                    raise

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

                try:
                    self.assertEqual(
                        1,
                        sum(1 for d in reg._local_dir.iterdir() if d.is_file())
                    )
                except AssertionError:
                    raise

                self.assertTrue(np.all(
                    np.arange(30) ==
                    reg.blk(ApriInfo(maybe="maybe"), 0, 30).segment()
                ))

                with self.assertRaises(DataNotFoundError):
                    reg.blk(ApriInfo(none="all"), 0, 14)

    def test__get_apri_json_by_id(self):

        reg = Testy_Register(SAVES_DIR, "hello")
        with reg.open() as reg:
            apri1 = ApriInfo(name ="hi")
            _id1 = reg._get_id_by_apri(apri1, None, True)

            self.assertIsInstance(
                _id1,
                bytes
            )
            self.assertEqual(
                apri1,
                ApriInfo.from_json(reg._get_apri_json_by_id(_id1).decode("ASCII"))
            )

            apri2 = ApriInfo(name ="sup")
            _id2 = reg._get_id_by_apri(apri2, None, True)
            self.assertEqual(
                apri2,
                ApriInfo.from_json(reg._get_apri_json_by_id(_id2).decode("ASCII"))
            )

    def test_apri_infos_no_recursive(self):

        reg = Testy_Register(SAVES_DIR, "msg")
        with self.assertRaisesRegex(RegisterError, "apris"):
            reg.apris()

        reg = Testy_Register(SAVES_DIR, "msg")
        with reg.open() as reg:

            apri1 = ApriInfo(name ="hello")
            reg._get_id_by_apri(apri1, None, True)
            self.assertEqual(
                1,
                len(list(reg.apris()))
            )
            self.assertEqual(
                apri1,
                list(reg.apris())[0]
            )

            apri2 = ApriInfo(name ="hey")
            blk = Block([], apri2)
            reg.add_ram_blk(blk)

            self.assertEqual(
                2,
                len(list(reg.apris()))
            )
            self.assertIn(
                apri1,
                list(reg.apris())
            )
            self.assertIn(
                apri2,
                list(reg.apris())
            )

    def test__open_created(self):

        reg = Testy_Register(SAVES_DIR, "testy")
        with reg.open() as reg: pass
        with reg.open() as reg:
            self.assertFalse(reg._db_is_closed())
            with self.assertRaises(RegisterAlreadyOpenError):
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
        apri1 = ApriInfo(name ="hello")
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

            apri1 = ApriInfo(name ="hey")
            blk1 = Block([], apri1)
            reg.add_disk_blk(blk1)
            with lmdb_prefix_iter(reg._db, _BLK_KEY_PREFIX) as it:
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
            reg.add_disk_blk(blk2)
            with lmdb_prefix_iter(reg._db, _BLK_KEY_PREFIX) as it:
                for key,_val in it:
                    if key not in old_keys:
                        curr_key = key
            self.assertEqual(
                (apri1, 0, 10),
                reg._convert_disk_block_key(_BLK_KEY_PREFIX_LEN, curr_key)
            )
            old_keys.add(curr_key)

            apri2 = ApriInfo(name ="hello")
            blk3 = Block(list(range(100)), apri2, 10)
            reg.add_disk_blk(blk3)
            with lmdb_prefix_iter(reg._db, _BLK_KEY_PREFIX) as it:
                for key,_val in it:
                    if key not in old_keys:
                        curr_key = key
            self.assertEqual(
                (apri2, 10, 100),
                reg._convert_disk_block_key(_BLK_KEY_PREFIX_LEN, curr_key)
            )
            old_keys.add(curr_key)

            blk4 = Block(list(range(100)), apri2)
            reg.add_disk_blk(blk4)
            with lmdb_prefix_iter(reg._db, _BLK_KEY_PREFIX) as it:
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
            reg._startn_tail_mod
        )
        self.assertEqual(
            head,
            reg._startn_head
        )
        self.assertEqual(
            tail_length,
            reg._startn_tail_length
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
        with self.assertRaisesRegex(RegisterError, "set_startn_info"):
            reg.set_startn_info(10, 3)

        reg = Testy_Register(SAVES_DIR, "hello")
        with reg.open() as reg:
            with self.assertRaisesRegex(TypeError, "int"):
                reg.set_startn_info(10, 3.5)

        reg = Testy_Register(SAVES_DIR, "hello")
        with reg.open() as reg:
            with self.assertRaisesRegex(TypeError, "int"):
                reg.set_startn_info(10.5, 3)

        reg = Testy_Register(SAVES_DIR, "hello")
        with reg.open() as reg:
            with self.assertRaisesRegex(ValueError, "non-negative"):
                reg.set_startn_info(-1, 3)

        reg = Testy_Register(SAVES_DIR, "hello")
        with reg.open() as reg:
            try:
                reg.set_startn_info(0, 3)
            except ValueError:
                self.fail("head can be 0")

        reg = Testy_Register(SAVES_DIR, "hello")
        with reg.open() as reg:
            with self.assertRaisesRegex(ValueError, "positive"):
                reg.set_startn_info(0, -1)

        reg = Testy_Register(SAVES_DIR, "hello")
        with reg.open() as reg:
            with self.assertRaisesRegex(ValueError, "positive"):
                reg.set_startn_info(0, 0)


        for head, tail_length in product([0, 1, 10, 100, 1100, 450], [1,2,3,4,5]):

            # check set works
            reg = Testy_Register(SAVES_DIR, "hello")
            with reg.open() as reg:

                try:
                    reg.set_startn_info(head, tail_length)

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
            with reg.open(readonly= True) as reg:
                with self.assertRaisesRegex(RegisterError, "read-only"):
                    reg.set_startn_info(head, tail_length)

            # tests make sure ValueError is thrown for small smart_n
            # 0 and head * 10 ** tail_len - 1 are the two possible extremes of the small start_n
            if head > 0:
                for start_n in [0, head * 10 ** tail_length - 1]:
                    reg = Testy_Register(SAVES_DIR, "hello")
                    with reg.open() as reg:
                            blk = Block([], ApriInfo(name ="hi"), start_n)
                            reg.add_disk_blk(blk)
                            with self.assertRaisesRegex(ValueError, "correct head"):
                                reg.set_startn_info(head, tail_length)

                            # make sure it exits safely
                            self.check_reg_set_start_n_info(
                                reg,
                                10 ** _START_N_TAIL_LENGTH_DEFAULT, 0, _START_N_TAIL_LENGTH_DEFAULT
                            )

            # tests to make sure a few permissible start_n work
            smallest = head * 10 ** tail_length
            largest = smallest + 10 ** tail_length  - 1
            for start_n in [smallest, smallest + 1, smallest + 2, largest -2, largest -1, largest]:
                reg = Testy_Register(SAVES_DIR, "hello")
                apri = ApriInfo(name="hi")
                with reg.open() as reg:
                    blk = Block([], apri,start_n)
                    reg.add_disk_blk(blk)

                    for debug in [0, 1, 2]:

                        if debug == _NO_DEBUG:
                            reg.set_startn_info(head, tail_length)

                        else:

                            cornifer.registers._debug = debug

                            with self.assertRaises(KeyboardInterrupt):
                                reg.set_startn_info(head // 10, tail_length + 1)

                            cornifer.registers._debug = _NO_DEBUG

                        self.check_reg_set_start_n_info(
                            reg,
                            10 ** tail_length, head, tail_length
                        )

                        with lmdb_prefix_iter(reg._db, _BLK_KEY_PREFIX) as it:
                            for curr_key,_ in it:pass

                        self.check_key_set_start_n_info(
                            reg, curr_key,
                            apri, start_n, 0
                        )

            # tests to make sure `largest + 1` etc do not work
            for start_n in [largest + 1, largest + 10, largest + 100, largest + 1000]:
                reg = Testy_Register(SAVES_DIR, "hello")
                apri = ApriInfo(name="hi")
                with reg.open() as reg:
                    blk = Block([], apri, start_n)
                    reg.add_disk_blk(blk)
                    with self.assertRaisesRegex(ValueError, "correct head"):
                        reg.set_startn_info(head, tail_length)

                    # make sure it exits safely
                    self.check_reg_set_start_n_info(
                        reg,
                        10 ** _START_N_TAIL_LENGTH_DEFAULT, 0, _START_N_TAIL_LENGTH_DEFAULT
                    )

    def check__iter_disk_block_pairs(self, t, apri, start_n, length):
        self.assertEqual(
            3,
            len(t)
        )
        self.assertIsInstance(
            t[0],
            ApriInfo
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
            apri1 = ApriInfo(name ="abc")
            apri2 = ApriInfo(name ="xyz")
            blk1 = Block(list(range(50)), apri1, 0)
            blk2 = Block(list(range(50)), apri1, 50)
            blk3 = Block(list(range(500)), apri2, 1000)

            reg.add_disk_blk(blk1)
            total = 0
            for i, t in chain(
                enumerate(reg._iter_disk_blk_pairs(_BLK_KEY_PREFIX, apri1, None)),
                enumerate(reg._iter_disk_blk_pairs(_BLK_KEY_PREFIX, None, apri1.to_json().encode("ASCII")))
            ):
                total += 1
                if i == 0:
                    t = reg._convert_disk_block_key(_BLK_KEY_PREFIX_LEN, t[0], apri1)
                    self.check__iter_disk_block_pairs(t, apri1, 0, 50)
                else:
                    self.fail()
            if total != 2:
                self.fail(str(total))

            reg.add_disk_blk(blk2)
            total = 0
            for i, t in chain(
                enumerate(reg._iter_disk_blk_pairs(_BLK_KEY_PREFIX, apri1, None)),
                enumerate(reg._iter_disk_blk_pairs(_BLK_KEY_PREFIX, None, apri1.to_json().encode("ASCII")))
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
                self.fail(str(total))

            total = 0
            for i, t in chain(
                enumerate(reg._iter_disk_blk_pairs(_BLK_KEY_PREFIX, apri1, None)),
                enumerate(reg._iter_disk_blk_pairs(_BLK_KEY_PREFIX, None, apri1.to_json().encode("ASCII")))
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

    def test_open(self):

        reg1 = Testy_Register(SAVES_DIR, "msg")
        with reg1.open() as reg2:pass
        self.assertIs(
            reg1,
            reg2
        )

        try:
            with reg1.open() as reg1:pass

        except RegisterError:
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
            with reg4.open(readonly= True) as reg:pass

    def test__recursive_open(self):

        # must be created
        reg1 = Testy_Register(SAVES_DIR, "hello")

        with self.assertRaises(RegisterError):
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

            except RegisterError:
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

        with reg6.open(readonly= True) as reg6:

            with self.assertRaisesRegex(ValueError, "read-only"):
                with reg6._recursive_open(False):pass

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
            key = reg._get_disk_blk_key(_BLK_KEY_PREFIX, apri, None, start_n, length, False)
            with reg._db.begin() as txn:
                filename = Path(txn.get(key).decode("ASCII"))
            self.assertTrue((reg._local_dir / filename).exists())

    def test_remove_disk_block(self):

        reg1 = Testy_Register(SAVES_DIR, "hi")

        with self.assertRaisesRegex(RegisterError, "open.*rmv_disk_blk"):
            reg1.rmv_disk_blk(ApriInfo(name ="fooopy doooopy"), 0, 0)

        with reg1.open() as reg1:

            apri1 = ApriInfo(name ="fooopy doooopy")
            blk1 = Block(list(range(50)), apri1)
            reg1.add_disk_blk(blk1)
            self._remove_disk_block_helper(reg1, [(apri1, 0, 50)])

            reg1.rmv_disk_blk(apri1, 0, 50)
            self._remove_disk_block_helper(reg1, [])

            reg1.add_disk_blk(blk1)
            apri2 = ApriInfo(name ="fooopy doooopy2")
            blk2 = Block(list(range(100)), apri2, 1000)
            reg1.add_disk_blk(blk2)
            self._remove_disk_block_helper(reg1, [(apri1, 0, 50), (apri2, 1000, 100)])

            reg1.rmv_disk_blk(apri2, 1000, 100)
            self._remove_disk_block_helper(reg1, [(apri1, 0, 50)])

            reg1.rmv_disk_blk(apri1, 0, 50)
            self._remove_disk_block_helper(reg1, [])

        with self.assertRaisesRegex(RegisterError, "read-write"):
            with reg1.open(readonly= True) as reg1:
                reg1.rmv_disk_blk(apri1, 0, 0)

        # add the same block to two registers
        reg1 = Testy_Register(SAVES_DIR, "hello")
        reg2 = Testy_Register(SAVES_DIR, "sup")
        apri = ApriInfo(name ="hi")
        blk = Block([], apri)

        with reg1.open() as reg1:
            reg1.add_disk_blk(blk)

        with reg2.open() as reg2:
            reg2.add_disk_blk(blk)

        with reg1.open() as reg1:
            reg1.rmv_disk_blk(apri, 0, 0)
            self._remove_disk_block_helper(reg1, [])

        with reg2.open() as reg2:
            self._remove_disk_block_helper(reg2, [(apri, 0, 0)])

        reg = NumpyRegister(SAVES_DIR, "hello")

        with reg.open() as reg:

            apri = ApriInfo(no ="yes")
            blk = Block(np.arange(14), apri)

            reg.add_disk_blk(blk)

            apri = ApriInfo(maybe ="maybe")
            blk = Block(np.arange(20), apri)

            reg.add_disk_blk(blk)

            for compress in range(2):

                for debug in [1,2,3,4,5,6,7,8,9,   12,13,14,15,16,17]:

                    if debug >= 9 and compress == 1 or debug == 9 and compress == 0:
                        continue

                    if compress == 1:
                        reg.compress(blk.apri(), blk.startn(), len(blk))

                    cornifer.registers._debug = debug

                    try:
                        with self.assertRaises(KeyboardInterrupt):
                            reg.rmv_disk_blk(ApriInfo(maybe ="maybe"), 0, 20)
                    except (RegisterRecoveryError, AssertionError):
                        raise

                    cornifer.registers._debug = _NO_DEBUG

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

                    try:
                        self.assertEqual(
                            2 + compress,
                            sum(1 for d in reg._local_dir.iterdir() if d.is_file())
                        )
                    except AssertionError:
                        raise

                    if compress == 1:
                        reg.decompress(blk.apri(), blk.startn(), len(blk))

                    self.assertTrue(np.all(
                        np.arange(14) ==
                        reg.blk(ApriInfo(no="yes"), 0, 14).segment()
                    ))

                    self.assertTrue(np.all(
                        np.arange(20) ==
                        reg.blk(ApriInfo(maybe="maybe"), 0, 20).segment()
                    ))

    def test_set_apos_info(self):

        reg = Testy_Register(SAVES_DIR, "hello")

        with self.assertRaisesRegex(RegisterError, "open.*set_apos"):
            reg.set_apos(ApriInfo(no ="no"), AposInfo(yes ="yes"))

        with reg.open() as reg:

            try:
                reg.set_apos(ApriInfo(no ="no"), AposInfo(yes ="yes"))

            except DataNotFoundError:
                self.fail("Do not need apri_info to already be there to add apos")

            except Exception as e:
                raise e

            self.assertEqual(
                1,
                lmdb_count_keys(reg._db, _APOS_KEY_PREFIX)
            )

            reg.set_apos(ApriInfo(no="no"), AposInfo(maybe="maybe"))

            self.assertEqual(
                1,
                lmdb_count_keys(reg._db, _APOS_KEY_PREFIX)
            )

            reg.set_apos(ApriInfo(weird="right"), AposInfo(maybe="maybe"))

            self.assertEqual(
                2,
                lmdb_count_keys(reg._db, _APOS_KEY_PREFIX)
            )

            reg.set_apos(ApriInfo(weird="right"), AposInfo(maybe="maybe"))

            self.assertEqual(
                2,
                lmdb_count_keys(reg._db, _APOS_KEY_PREFIX)
            )

            for debug in [1,2]:

                cornifer.registers._debug = debug

                with self.assertRaises(KeyboardInterrupt):
                    reg.set_apos(ApriInfo(__ ="____"), AposInfo(eight = 9))

                cornifer.registers._debug = _NO_DEBUG

                self.assertEqual(
                    2,
                    lmdb_count_keys(reg._db, _APOS_KEY_PREFIX)
                )

        with reg.open(readonly= True) as reg:
            with self.assertRaisesRegex(RegisterError, "read-write"):
                reg.set_apos(ApriInfo(no="no"), AposInfo(yes="yes"))

    def test_apos_info(self):

        reg = Testy_Register(SAVES_DIR, "hello")

        with self.assertRaisesRegex(RegisterError, "open.*apos"):
            reg.apos(ApriInfo(no ="no"))

        with reg.open() as reg:

            apri = ApriInfo(no ="yes")
            apos = AposInfo(yes ="no")

            with self.assertRaisesRegex(DataNotFoundError, re.escape(str(apri))):
                reg.apos(apri)

            reg.set_apos(apri, apos)

            self.assertEqual(
                apos,
                reg.apos(apri)
            )

            apri = ApriInfo(no ="yes")
            apos = AposInfo(yes ="no", restart = AposInfo(num = 1))

            reg.set_apos(apri, apos)

            self.assertEqual(
                apos,
                reg.apos(apri)
            )

        with reg.open(readonly= True) as reg:

            try:
                self.assertEqual(
                    apos,
                    reg.apos(apri)
                )

            except RegisterError as e:

                if "read-write" in str(e):
                    self.fail("apos allows the register to be in read-only mode")

                else:
                    raise e

            except Exception as e:
                raise e

    def test_remove_apos_info(self):

        reg = Testy_Register(SAVES_DIR, "hello")

        with self.assertRaisesRegex(RegisterError, "open.*rmv_apos"):
            reg.rmv_apos(ApriInfo(no ="no"))

        with reg.open() as reg:

            apri1 = ApriInfo(no ="yes")
            apos1 = AposInfo(yes ="no")

            apri2 = ApriInfo(maam ="sir")
            apos2 = AposInfo(sir ="maam", restart = apos1)

            reg.set_apos(apri1, apos1)

            reg.rmv_apos(apri1)

            self.assertEqual(
                0,
                lmdb_count_keys(reg._db, _APOS_KEY_PREFIX)
            )

            with self.assertRaisesRegex(DataNotFoundError, re.escape(str(apri1))):
                reg.apos(apri1)

            reg.set_apos(apri1, apos1)
            reg.set_apos(apri2, apos2)

            reg.rmv_apos(apri2)

            self.assertEqual(
                1,
                lmdb_count_keys(reg._db, _APOS_KEY_PREFIX)
            )

            with self.assertRaisesRegex(DataNotFoundError, re.escape(str(apri2))):
                reg.apos(apri2)

            self.assertEqual(
                apos1,
                reg.apos(apri1)
            )

            for debug in [1,2]:

                cornifer.registers._debug = debug

                with self.assertRaises(KeyboardInterrupt):
                    reg.rmv_apos(apri1)

                cornifer.registers._debug = _NO_DEBUG

                self.assertEqual(
                    1,
                    lmdb_count_keys(reg._db, _APOS_KEY_PREFIX)
                )

                self.assertEqual(
                    apos1,
                    reg.apos(apri1)
                )



        with reg.open(readonly= True) as reg:
            with self.assertRaisesRegex(RegisterError, "read-write"):
                reg.rmv_apos(apri1)

    def test_disk_blocks_no_recursive(self):

        reg = NumpyRegister(SAVES_DIR, "HI")
        with reg.open() as reg:
            apri1 = ApriInfo(name ="abc")
            apri2 = ApriInfo(name ="xyz")
            blk1 = Block(np.arange(50), apri1, 0)
            blk2 = Block(np.arange(50), apri1, 50)
            blk3 = Block(np.arange(500), apri2, 1000)

            reg.add_disk_blk(blk1)
            total = 0
            for i, blk in enumerate(reg.blks(apri1)):
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

            reg.add_disk_blk(blk2)
            total = 0
            for i, blk in enumerate(reg.blks(apri1)):
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

            reg.add_disk_blk(blk3)
            total = 0
            for i, blk in enumerate(reg.blks(apri1)):
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
            for i,blk in enumerate(reg.blks(apri2)):
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
            for i,_ in enumerate(reg._iter_subregs()):
                total += 1
            self.assertEqual(
                0,
                total
            )


        reg = Testy_Register(SAVES_DIR, "hello")

        with reg.open() as reg:

            with reg._db.begin(write = True) as txn:
                txn.put(reg._get_subreg_key(), _SUB_VAL)

            total = 0
            for i, _reg in enumerate(reg._iter_subregs()):
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
                txn.put(reg2._get_subreg_key(), _SUB_VAL)
                txn.put(reg3._get_subreg_key(), _SUB_VAL)

            total = 0
            regs = []
            for i, _reg in enumerate(reg1._iter_subregs()):
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
                txn.put(reg3._get_subreg_key(), _SUB_VAL)

        with reg1.open() as reg:

            with reg1._db.begin(write=True) as txn:
                txn.put(reg2._get_subreg_key(), _SUB_VAL)

            total = 0
            regs = []
            for i, _reg in enumerate(reg._iter_subregs()):
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
            for i, _reg in enumerate(reg._iter_subregs()):
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

    def test_blkByN(self):

        reg = Testy_Register(SAVES_DIR, "hello")
        with self.assertRaisesRegex(RegisterError, "open.*blk_by_n"):
            reg.blk_by_n(ApriInfo(name ="no"), -1)

        reg = Testy_Register(SAVES_DIR, "hello")
        with self.assertRaisesRegex(IndexError, "non-negative"):
            with reg.open() as reg:
                reg.blk_by_n(ApriInfo(name ="no"), -1)

        reg = Testy_Register(SAVES_DIR, "hello")
        apri = ApriInfo(name ="list")
        blk1 = Block(list(range(1000)), apri)
        with reg.open() as reg:
            reg.add_ram_blk(blk1)
            for n in [0, 10, 500, 990, 999]:
                self.assertIs(
                    blk1,
                    reg.blk_by_n(apri, n)
                )
            with self.assertRaises(DataNotFoundError):
                reg.blk_by_n(apri, 1000)

        blk2 = Block(list(range(1000, 2000)), apri, 1000)
        with reg.open() as reg:
            reg.add_ram_blk(blk2)
            for n in [1000, 1010, 1990, 1999]:
                self.assertIs(
                    blk2,
                    reg.blk_by_n(apri, n)
                )

        reg = Testy_Register(SAVES_DIR, "whatever")

        apri = ApriInfo(name ="whatev")
        with reg.open() as reg: pass
        with self.assertRaisesRegex(RegisterError, "open.*blk_by_n"):
            for _ in reg.blk_by_n(apri, 0): pass

        apri1 = ApriInfo(name ="foomy")
        apri2 = ApriInfo(name ="doomy")
        blk1 = Block(list(range(10)), apri1)
        blk2 = Block(list(range(20)), apri1, 10)
        blk3 = Block(list(range(14)), apri2, 50)
        blk4 = Block(list(range(100)), apri2, 120)
        blk5 = Block(list(range(120)), apri2, 1000)
        reg1 = Testy_Register(SAVES_DIR, "helllo")
        reg2 = Testy_Register(SAVES_DIR, "suuup")

        with reg1.open() as reg1:

            reg1.add_ram_blk(blk1)
            reg1.add_ram_blk(blk2)
            reg1.add_ram_blk(blk3)

        with reg2.open() as reg2:

            reg2.add_ram_blk(blk4)
            reg2.add_ram_blk(blk5)


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
            with reg.open() as reg:
                try:
                    self.assertIs(
                        blk,
                        reg.blk_by_n(*args[:2])
                    )
                except:
                    raise

        reg = NumpyRegister(SAVES_DIR, "hello")
        with self.assertRaises(RegisterError):
            reg.blk_by_n(ApriInfo(name="no"), 50)

        reg = NumpyRegister(SAVES_DIR, "hello")
        apri1 = ApriInfo(name ="sup")
        apri2 = ApriInfo(name ="hi")
        blk1 = Block(np.arange(75), apri1)
        blk2 = Block(np.arange(125), apri1, 75)
        blk3 = Block(np.arange(1000), apri2, 100)
        blk4 = Block(np.arange(100), apri2, 2000)
        with reg.open() as reg:
            reg.add_disk_blk(blk1)
            reg.add_disk_blk(blk2)
            reg.add_disk_blk(blk3)
            reg.add_disk_blk(blk4)
            for n in [0, 1, 2, 72, 73, 74]:
                self.assertEqual(
                    blk1,
                    reg.blk_by_n(apri1, n)
                )
            for n in [75, 76, 77, 197, 198, 199]:
                self.assertEqual(
                    blk2,
                    reg.blk_by_n(apri1, n)
                )
            for n in [-2, -1]:
                with self.assertRaisesRegex(IndexError, "non-negative"):
                    reg.blk_by_n(apri1, n)
            for n in [200, 201, 1000]:
                with self.assertRaises(DataNotFoundError):
                    reg.blk_by_n(apri1, n)

    def test_blk(self):

        reg = NumpyRegister(SAVES_DIR, "hello")
        with self.assertRaisesRegex(RegisterError, "blk"):
            reg.blk(ApriInfo(name="i am the octopus"), 0, 0)

        reg = NumpyRegister(SAVES_DIR, "hello")
        with reg.open() as reg:
            apri1 = ApriInfo(name ="i am the octopus")
            blk1 = Block(np.arange(100), apri1)
            reg.add_disk_blk(blk1)

            self.assertEqual(
                blk1,
                reg.blk(apri1, 0, 100)
            )

            blk2 = Block(np.arange(100,200), apri1, 100)
            reg.add_disk_blk(blk2)

            self.assertEqual(
                blk2,
                reg.blk(apri1, 100, 100)
            )

            self.assertEqual(
                blk1,
                reg.blk(apri1, 0, 100)
            )

            apri2 = ApriInfo(name ="hello")
            blk3 = Block(np.arange(3000,4000), apri2, 2000)
            reg.add_disk_blk(blk3)

            self.assertEqual(
                blk3,
                reg.blk(apri2, 2000, 1000)
            )

            self.assertEqual(
                blk2,
                reg.blk(apri1, 100, 100)
            )

            self.assertEqual(
                blk1,
                reg.blk(apri1, 0, 100)
            )

            for metadata in [
                (apri1, 0, 200), (apri1, 1, 99), (apri1, 5, 100), (apri1, 1, 100),
                (apri2, 2000, 999), (apri2, 2000, 1001), (apri2, 1999, 1000),
                (ApriInfo(name ="noooo"), 0, 100)
            ]:
                with self.assertRaises(DataNotFoundError):
                    reg.blk(*metadata)

            apri3 = ApriInfo(
                name = "'''i love quotes'''and'' backslashes\\\\",
                num = '\\\"double\\quotes\' are cool too"'
            )
            blk = Block(np.arange(69, 420), apri3)
            reg.add_disk_blk(blk)

            self.assertEqual(
                blk,
                reg.blk(apri3, 0, 420 - 69)
            )

        reg = NumpyRegister(SAVES_DIR, "tests")

        apri1 = ApriInfo(descr ="hey")

        with self.assertRaisesRegex(RegisterError, "open.*blk"):
            reg.blk(apri1)

        with reg.open() as reg:

            with self.assertRaisesRegex(TypeError, "ApriInfo"):
                reg.blk("kitty kat")

            with self.assertRaisesRegex(TypeError, "int"):
                reg.blk(apri1, "puppy dawg")

            with self.assertRaisesRegex(TypeError, "int"):
                reg.blk(apri1, 0, "bunny wunny")

            with self.assertRaisesRegex(ValueError, "non-negative"):
                reg.blk(apri1, -1)

            with self.assertRaisesRegex(ValueError, "non-negative"):
                reg.blk(apri1, 0, -1)

            with self.assertRaises(ValueError):
                reg.blk(apri1, length=-1)

            reg.add_disk_blk(Block(list(range(50)), apri1))

            self.assertTrue(np.all(
                reg.blk(apri1).segment() == np.arange(50)
            ))

            self.assertTrue(np.all(
                reg.blk(apri1, 0).segment() == np.arange(50)
            ))

            self.assertTrue(np.all(
                reg.blk(apri1, 0, 50).segment() == np.arange(50)
            ))

            reg.add_disk_blk(Block(list(range(51)), apri1))

            self.assertTrue(np.all(
                reg.blk(apri1).segment() == np.arange(51)
            ))

            self.assertTrue(np.all(
                reg.blk(apri1, 0).segment() == np.arange(51)
            ))

            self.assertTrue(np.all(
                reg.blk(apri1, 0, 51).segment() == np.arange(51)
            ))

            self.assertTrue(np.all(
                reg.blk(apri1, 0, 50).segment() == np.arange(50)
            ))

            reg.add_disk_blk(Block(list(range(100)), apri1, 1))

            self.assertTrue(np.all(
                reg.blk(apri1).segment() == np.arange(51)
            ))

            self.assertTrue(np.all(
                reg.blk(apri1, 0).segment() == np.arange(51)
            ))

            self.assertTrue(np.all(
                reg.blk(apri1, 0, 51).segment() == np.arange(51)
            ))

            self.assertTrue(np.all(
                reg.blk(apri1, 0, 50).segment() == np.arange(50)
            ))

            self.assertTrue(np.all(
                reg.blk(apri1, 1, 100).segment() == np.arange(100)
            ))

            blk1 = Block(list(range(5)), apri1)
            reg.add_ram_blk(blk1)

            self.assertIs(
                blk1,
                reg.blk(apri1)
            )

            self.assertIs(
                blk1,
                reg.blk(apri1, 0)
            )

            self.assertIs(
                blk1,
                reg.blk(apri1, 0, 5)
            )

    def test__check_no_cycles_from(self):

        reg = Testy_Register(SAVES_DIR, "hello")
        with self.assertRaises(RegisterError):
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
                txn.put(reg2._get_subreg_key(), _SUB_VAL)

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
                txn.put(reg3._get_subreg_key(), _SUB_VAL)

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
                txn.put(reg5._get_subreg_key(), _SUB_VAL)

        with reg5.open() as reg5:

            with reg5._db.begin(write=True) as txn:
                txn.put(reg4._get_subreg_key(), _SUB_VAL)

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
                txn.put(reg6._get_subreg_key(), _SUB_VAL)

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
                txn.put(reg1._get_subreg_key(), _SUB_VAL)

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

        regs = [NumpyRegister(SAVES_DIR, f"{i}") for i in range(N + 2)]

        for reg in regs:
            with reg.open():pass

        for i in range(N):
            with regs[i].open() as reg:
                with reg._db.begin(write=True) as txn:
                    txn.put(regs[i+1]._get_subreg_key(), _SUB_VAL)

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
                regs[i]._check_no_cycles_from(regs[N + 1])
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
        with self.assertRaisesRegex(RegisterError, "open.*add_subreg"):
            reg1.add_subreg(reg2)

        reg1 = Testy_Register(SAVES_DIR, "hello")
        reg2 = Testy_Register(SAVES_DIR, "hello")
        with reg1.open() as reg1:
            with self.assertRaisesRegex(RegisterError, "add_subreg"):
                reg1.add_subreg(reg2)

        reg1 = Testy_Register(SAVES_DIR, "hello")
        reg2 = Testy_Register(SAVES_DIR, "hello")
        reg3 = Testy_Register(SAVES_DIR, "hello")
        with reg2.open(): pass
        with reg1.open() as reg1:
            try:
                reg1.add_subreg(reg2)
            except RegisterError:
                self.fail()

        with reg3.open(): pass

        with self.assertRaisesRegex(RegisterError, "read-write"):
            with reg2.open(readonly= True) as reg2:
                reg2.add_subreg(reg3)

        with reg2.open() as reg2:
            try:
                reg2.add_subreg(reg3)
            except RegisterError:
                self.fail()
        with reg1.open() as reg1:
            try:
                reg1.add_subreg(reg3)
            except RegisterError:
                self.fail()

        reg1 = Testy_Register(SAVES_DIR, "hello")
        reg2 = Testy_Register(SAVES_DIR, "hello")
        reg3 = Testy_Register(SAVES_DIR, "hello")
        with reg3.open(): pass
        with reg2.open() as reg2:
            try:
                reg2.add_subreg(reg3)
            except RegisterError:
                self.fail()
        with reg1.open() as reg1:
            try:
                reg1.add_subreg(reg2)
            except RegisterError:
                self.fail()
        with reg3.open() as reg3:
            with self.assertRaises(RegisterError):
                reg3.add_subreg(reg1)

        reg1 = Testy_Register(SAVES_DIR, "hello")
        reg2 = Testy_Register(SAVES_DIR, "hello")

        with reg1.open():pass
        with reg2.open():pass

        with reg1.open() as reg1:

            for debug in [1,2]:

                cornifer.registers._debug = debug

                with self.assertRaises(KeyboardInterrupt):
                        reg1.add_subreg(reg2)

                cornifer.registers._debug = _NO_DEBUG

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

        with self.assertRaisesRegex(RegisterError, "open.*rmv_subreg"):
            reg1.rmv_subreg(reg2)

        with reg3.open():pass

        with reg1.open() as reg1:

            reg1.add_subreg(reg2)
            self.assertEqual(
                1,
                lmdb_count_keys(reg1._db, _SUB_KEY_PREFIX)
            )

            reg1.rmv_subreg(reg2)
            self.assertEqual(
                0,
                lmdb_count_keys(reg1._db, _SUB_KEY_PREFIX)
            )

            reg1.add_subreg(reg2)
            reg1.add_subreg(reg3)
            self.assertEqual(
                2,
                lmdb_count_keys(reg1._db, _SUB_KEY_PREFIX)
            )

            reg1.rmv_subreg(reg2)
            self.assertEqual(
                1,
                lmdb_count_keys(reg1._db, _SUB_KEY_PREFIX)
            )

            for debug in [1,2]:

                cornifer.registers._debug = debug

                with self.assertRaises(KeyboardInterrupt):
                    reg1.rmv_subreg(reg3)

                cornifer.registers._debug = _NO_DEBUG

                self.assertEqual(
                    1,
                    lmdb_count_keys(reg1._db, _SUB_KEY_PREFIX)
                )

            reg1.rmv_subreg(reg3)
            self.assertEqual(
                0,
                lmdb_count_keys(reg1._db, _SUB_KEY_PREFIX)
            )

        with self.assertRaisesRegex(RegisterError, "read-write"):

            with reg1.open(readonly= True) as reg1:
                reg1.rmv_subreg(reg2)

    def test_blks(self):

        reg = Testy_Register(SAVES_DIR, "whatever")
        apri = ApriInfo(name ="whatev")

        with self.assertRaisesRegex(RegisterError, "open.*blks"):
            list(reg.blks(apri))

        reg = Testy_Register(SAVES_DIR, "whatever")
        apri1 = ApriInfo(name ="foomy")
        apri2 = ApriInfo(name ="doomy")
        blk1 = Block(list(range(10)), apri1)
        blk2 = Block(list(range(20)), apri1, 10)
        blk3 = Block(list(range(14)), apri2, 50)
        blk4 = Block(list(range(100)), apri2, 120)
        blk5 = Block(list(range(120)), apri2, 1000)
        reg1 = Testy_Register(SAVES_DIR, "helllo")
        reg2 = Testy_Register(SAVES_DIR, "suuup")

        with reg1.open() as reg1:

            reg1.add_ram_blk(blk1)
            reg1.add_ram_blk(blk2)
            reg1.add_ram_blk(blk3)

        with reg2.open() as reg2:

            reg2.add_ram_blk(blk4)
            reg2.add_ram_blk(blk5)

        with reg1.open() as reg1:

            total = 0
            for i, blk in enumerate(reg1.blks(apri1)):
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
            reg1.add_subreg(reg2)
            total = 0
            for i, blk in enumerate(reg1.blks(apri1, recursively = True)):
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
            for i, blk in enumerate(reg1.blks(apri2, recursively = True)):
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

        reg = Testy_Register(SAVES_DIR, "msg")
        apri1 = ApriInfo(name="hey")
        blk1 = Block([], apri1)

        with reg.open() as reg:

            reg.add_ram_blk(blk1)
            self.assertEqual(
                1,
                len(list(reg.blks(apri1)))
            )
            self.assertEqual(
                blk1,
                list(reg.blks(apri1))[0]
            )

            apri2 = ApriInfo(name ="hello")
            blk2 = Block(list(range(10)), apri2)
            reg.add_ram_blk(blk2)
            self.assertEqual(
                1,
                len(list(reg.blks(apri2)))
            )
            self.assertEqual(
                blk2,
                list(reg.blks(apri2))[0]
            )

            blk3 = Block(list(range(10)), apri2, 1)
            reg.add_ram_blk(blk3)
            self.assertEqual(
                2,
                len(list(reg.blks(apri2)))
            )
            self.assertIn(
                blk2,
                reg.blks(apri2)
            )
            self.assertIn(
                blk3,
                reg.blks(apri2)
            )

    def test_intervals(self):

        reg = Testy_Register(SAVES_DIR, "sup")

        apri1 = ApriInfo(descr ="hello")
        apri2 = ApriInfo(descr ="hey")

        with self.assertRaisesRegex(RegisterError, "open.*intervals"):
            list(reg.intervals(apri1))

        with reg.open() as reg:

            for apri in [apri1, apri2]:

                with self.assertRaisesRegex(DataNotFoundError, "ApriInfo"):
                    list(reg.intervals(apri, combine=False, diskonly=True))


        with reg.open() as reg:

            reg.add_disk_blk(Block(list(range(50)), apri1))

            self.assertEqual(
                [(0, 50)],
                list(reg.intervals(apri1, combine=False, diskonly=True))
            )

            with self.assertRaisesRegex(DataNotFoundError, "ApriInfo"):
                list(reg.intervals(apri2, combine=False, diskonly=True))

            reg.add_disk_blk(Block(list(range(100)), apri1))

            self.assertEqual(
                [(0, 100), (0, 50)],
                list(reg.intervals(apri1, combine=False, diskonly=True))
            )

            reg.add_disk_blk(Block(list(range(1000)), apri1, 1))

            self.assertEqual(
                [(0, 100), (0, 50), (1, 1000)],
                list(reg.intervals(apri1, combine=False, diskonly=True))
            )

            reg.add_disk_blk(Block(list(range(420)), apri2, 69))

            self.assertEqual(
                [(0, 100), (0, 50), (1, 1000)],
                list(reg.intervals(apri1, combine=False, diskonly=True))
            )

            self.assertEqual(
                [(69, 420)],
                list(reg.intervals(apri2, combine=False, diskonly=True))
            )

        # blk = Block(list(range(50)), )

    def test__iter_ram_and_disk_block_datas(self):pass

    def test_apri_infos(self):

        reg = Testy_Register(SAVES_DIR, "tests")

        with self.assertRaisesRegex(RegisterError, "open.*apris"):
            reg.apris()

        for i in range(200):

            apri1 = ApriInfo(name = i)
            apri2 = ApriInfo(name =f"{i}")

            with reg.open() as reg:

                reg.add_disk_blk(Block([1], apri1))
                reg.add_ram_blk(Block([1], apri2))

                get = reg.apris()

            self.assertEqual(
                2*(i+1),
                len(get)
            )

            for j in range(i+1):

                self.assertIn(
                    ApriInfo(name = i),
                    get
                )

                self.assertIn(
                    ApriInfo(name =f"{i}"),
                    get
                )

    def _is_compressed_helper(self, reg, apri, start_n, length, data_file_bytes = None):

        compressed_key = reg._get_disk_blk_key(_COMPRESSED_KEY_PREFIX, apri, None, start_n, length, False)

        self.assertTrue(lmdb_has_key(reg._db, compressed_key))

        with reg._db.begin() as txn:
            val = txn.get(compressed_key)

        self.assertNotEqual(val, _IS_NOT_COMPRESSED_VAL)

        zip_filename = (reg._local_dir / val.decode("ASCII")).with_suffix(".zip")

        self.assertTrue(zip_filename.exists())

        self.assertEqual(zip_filename.suffix, ".zip")

        data_key = reg._get_disk_blk_key(_BLK_KEY_PREFIX, apri, None, start_n, length, False)

        self.assertTrue(lmdb_has_key(reg._db, data_key))

        if data_file_bytes is not None:

            with reg._db.begin() as txn:
                self.assertEqual(txn.get(data_key), data_file_bytes)

            data_filename = reg._local_dir / data_file_bytes.decode("ASCII")

            self.assertTrue(data_filename.exists())

            self.assertLessEqual(os.stat(data_filename).st_size, 2)

    def _is_not_compressed_helper(self, reg, apri, start_n, length):

        compressed_key = reg._get_disk_blk_key(_COMPRESSED_KEY_PREFIX, apri, None, start_n, length, False)

        self.assertTrue(lmdb_has_key(reg._db, compressed_key))

        with reg._db.begin() as txn:
            self.assertEqual(txn.get(compressed_key), _IS_NOT_COMPRESSED_VAL)

        data_key = reg._get_disk_blk_key(_BLK_KEY_PREFIX, apri, None, start_n, length, False)

        with reg._db.begin() as txn:
            return txn.get(data_key)

    def test_compress(self):

        reg2 = NumpyRegister(SAVES_DIR, "testy2")

        with self.assertRaisesRegex(RegisterError, "open.*compress"):
            reg2.compress(ApriInfo(num = 0))

        apri1 = ApriInfo(descr ="sup")
        apri2 = ApriInfo(descr ="hey")
        apris = [apri1, apri1, apri2]

        length1 = 500
        blk1 = Block(np.arange(length1), apri1)
        length2 = 1000000
        blk2 = Block(np.arange(length2), apri1)
        length3 = 2000
        blk3 = Block(np.arange(length3), apri2)
        lengths = [length1, length2, length3]

        with reg2.open() as reg2:
            reg2.add_disk_blk(blk1)
            reg2.add_disk_blk(blk2)
            reg2.add_disk_blk(blk3)

            for i, (apri, length) in enumerate(zip(apris, lengths)):

                data_file_bytes = self._is_not_compressed_helper(reg2, apri, 0, length)
                reg2.compress(apri, 0, length)
                self._is_compressed_helper(reg2, apri, 0, length, data_file_bytes)

                for _apri, _length in zip(apris[i+1:], lengths[i+1:]):

                    self._is_not_compressed_helper(reg2, _apri, 0, _length)

                expected = str(apri).replace("(", "\\(").replace(")", "\\)") + f".*startn.*0.*length.*{length}"

                with self.assertRaisesRegex(CompressionError, expected):
                    reg2.compress(apri, 0, length)

        with self.assertRaisesRegex(RegisterError, "read-write"):
            with reg2.open(readonly= True) as reg2:
                reg2.compress(ApriInfo(num = 0))

        reg = NumpyRegister(SAVES_DIR, "no")

        with reg.open() as reg:

            apri = ApriInfo(num = 7)
            blk = Block(np.arange(40), apri)
            reg.add_disk_blk(blk)

            for debug in [1,2,3,4]:

                cornifer.registers._debug = debug

                with self.assertRaises(KeyboardInterrupt):
                    reg.compress(apri)

                cornifer.registers._debug = _NO_DEBUG

                self._is_not_compressed_helper(reg, apri, 0, 40)

    def test_decompress(self):

        reg1 = NumpyRegister(SAVES_DIR, "lol")

        apri1 = ApriInfo(descr ="LOL")
        apri2 = ApriInfo(decr ="HAHA")
        apris = [apri1, apri1, apri2]

        with self.assertRaisesRegex(RegisterError, "open.*decompress"):
            reg1.decompress(apri1)

        lengths = [50, 500, 5000]
        start_ns = [0, 0, 1000]

        data = [np.arange(length) for length in lengths]

        blks = [Block(*t) for t in zip(data, apris, start_ns)]

        data_files_bytes = []

        with reg1.open() as reg1:

            for blk in blks:
                reg1.add_disk_blk(blk)
                data_files_bytes.append(
                    self._is_not_compressed_helper(reg1, blk.apri(), blk.startn(), len(blk))
                )

            for t in zip(apris, start_ns, lengths):
                reg1.compress(*t)

            for i, t in enumerate(zip(apris, start_ns, lengths)):

                reg1.decompress(*t)

                self._is_not_compressed_helper(reg1, *t)

                for _t in zip(apris[i+1:], start_ns[i+1:], lengths[i+1:], data_files_bytes[i+1:]):

                    self._is_compressed_helper(reg1, *_t)

                expected = str(t[0]).replace("(", "\\(").replace(")", "\\)") + f".*startn.*0.*length.*{t[2]}"
                with self.assertRaisesRegex(DecompressionError, expected):
                    reg1.decompress(*t)

        with self.assertRaisesRegex(RegisterError, "read-only"):

            with reg1.open(readonly= True) as reg1:
                reg1.decompress(apri1)

        reg2 = NumpyRegister(SAVES_DIR, "hi")

        with reg2.open() as reg2:

            apri = ApriInfo(hi ="hello")
            blk1 = Block(np.arange(15), apri)
            blk2 = Block(np.arange(15, 30), apri, 15)

            reg2.add_disk_blk(blk1)
            reg2.add_disk_blk(blk2)

            reg2.compress(apri, 0, 15)
            reg2.compress(apri, 15, 15)

            for debug in [1, 2, 3, 4]:

                cornifer.registers._debug = debug

                with self.assertRaises(KeyboardInterrupt):
                    reg2.decompress(apri, 15, 15, False)

                cornifer.registers._debug = _NO_DEBUG

                with reg2._db.begin() as txn:

                    blk_filename1 = txn.get(reg2._get_disk_blk_key(_BLK_KEY_PREFIX, apri, None, 0, 15, False))
                    blk_filename2 = txn.get(reg2._get_disk_blk_key(_BLK_KEY_PREFIX, apri, None, 15, 15, False))

                    self._is_compressed_helper(reg2, apri, 0, 15, blk_filename1)
                    self._is_compressed_helper(reg2, apri, 15, 15, blk_filename2)

            reg2.decompress(apri, 0, 15)
            reg2.decompress(apri, 15, 15)

            self._is_not_compressed_helper(reg2, apri, 0, 15)
            self._is_not_compressed_helper(reg2, apri, 15, 15)


        # with reg2.open() as reg2:
        #
        #     reg2.add_disk_blk(Block(list(range(10)), apri1))
        #
        #     reg2.compress(apri1)
        #
        #     for key, value in reg2._iter_disk_blk_pairs(_COMPRESSED_KEY_PREFIX, apri1, None):
        #
        #         compr_filename = reg2._localDir / value.decode("ASCII")
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
        #     for _, value in reg2._iter_disk_blk_pairs(_BLK_KEY_PREFIX, apri1, None):
        #
        #         filename = reg2._localDir / value.decode("ASCII")
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
    #         with self.assertRaisesRegex(DataNotFoundError, expected):
    #             reg.compress_all(apri1)
    #
    #         reg.add_disk_blk(blk1)
    #
    #         data_file_bytes1 = self._is_not_compressed_helper(reg, apri1, 0, 10000)
    #
    #         reg.compress_all(apri1)
    #
    #         self._is_compressed_helper(reg, apri1, 0, 10000, data_file_bytes1)
    #
    #         reg.add_disk_blk(blk2)
    #         data_file_bytes2 = self._is_not_compressed_helper(reg, apri1, 0, 1000)
    #         reg.add_disk_blk(blk3)
    #         data_file_bytes3 = self._is_not_compressed_helper(reg, apri1, 42069, 30000)
    #         reg.add_disk_blk(blk4)
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
    #         with self.assertRaisesRegex(DataNotFoundError, expected):
    #             reg.decompress_all(apri1)
    #
    #         reg.add_disk_blk(blk1)
    #         reg.add_disk_blk(blk2)
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
    #         reg.add_disk_blk(blk3)
    #         reg.add_disk_blk(blk4)
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

        with self.assertRaisesRegex(RegisterError, "open.*change_apri"):
            reg.change_apri(ApriInfo(i = 0), ApriInfo(j=0))

        with reg.open() as reg:

            old_apri = ApriInfo(sup ="hey")
            new_apri = ApriInfo(hello ="hi")
            apos = AposInfo(hey ="sup")

            with self.assertRaisesRegex(DataNotFoundError, re.escape(str(old_apri))):
                reg.change_apri(old_apri, new_apri)

            reg.set_apos(old_apri, apos)

            reg.change_apri(old_apri, new_apri)

            with self.assertRaisesRegex(DataNotFoundError, re.escape(str(old_apri))):
                reg.apos(old_apri)

            self.assertEqual(
                apos,
                reg.apos(new_apri)
            )

            self.assertEqual(
                1,
                len(reg.apris())
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

        with reg.open(readonly= True) as reg:
            with self.assertRaisesRegex(RegisterError, "read-write"):
                reg.change_apri(old_apri, new_apri)

        reg = NumpyRegister(SAVES_DIR, "hello")

        with reg.open() as reg:

            old_apri = ApriInfo(sup ="hey")
            other_apri = ApriInfo(sir ="maam", respective = old_apri)
            new_apri = ApriInfo(hello ="hi")
            new_other_apri = ApriInfo(respective = new_apri, sir ="maam")

            apos1 = AposInfo(some ="info")
            apos2 = AposInfo(some_more ="info")

            reg.set_apos(old_apri, apos1)
            reg.set_apos(other_apri, apos2)

            reg.change_apri(old_apri, new_apri)

            with self.assertRaisesRegex(DataNotFoundError, re.escape(str(old_apri))):
                reg.apos(old_apri)

            with self.assertRaisesRegex(DataNotFoundError, re.escape(str(old_apri))):
                reg.apos(other_apri)

            self.assertEqual(
                apos1,
                reg.apos(new_apri)
            )

            self.assertEqual(
                apos2,
                reg.apos(new_other_apri)
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

            get = reg.apris()

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

            reg.change_apri(new_apri, old_apri)

            with self.assertRaisesRegex(DataNotFoundError, re.escape(str(new_apri))):
                reg.apos(new_apri)

            with self.assertRaisesRegex(DataNotFoundError, re.escape(str(new_apri))):
                reg.apos(new_other_apri)

            self.assertEqual(
                apos1,
                reg.apos(old_apri)
            )

            self.assertEqual(
                apos2,
                reg.apos(other_apri)
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

            get = reg.apris()

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


            # change to an info that already exists in the register

            blk = Block(np.arange(100), other_apri)
            reg.add_disk_blk(blk)

            reg.change_apri(old_apri, other_apri)

            other_other_apri = ApriInfo(sir ="maam", respective = other_apri)

            with self.assertRaisesRegex(DataNotFoundError, r"AposInfo.*" + re.escape(str(old_apri))):
                reg.apos(old_apri)

            try:
                reg.apos(other_apri)

            except DataNotFoundError:
                self.fail("It does contain other_apri")

            self.assertEqual(
                apos1,
                reg.apos(other_apri)
            )

            self.assertEqual(
                apos2,
                reg.apos(other_other_apri)
            )

            self.assertEqual(
                Block(np.arange(100), other_other_apri),
                reg.blk(other_other_apri)
            )

            self.assertIn(
                other_apri,
                reg
            )

            self.assertIn(
                other_other_apri,
                reg
            )

            self.assertIn(
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

            get = reg.apris()

            self.assertEqual(
                3,
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
                3,
                lmdb_count_keys(reg._db, _APRI_ID_KEY_PREFIX)
            )

            self.assertEqual(
                3,
                lmdb_count_keys(reg._db, _ID_APRI_KEY_PREFIX)
            )

            # change to an info that creates duplicate keys

            with self.assertRaisesRegex(ValueError, "[dD]uplicate"):
                reg.change_apri(other_other_apri, other_apri)

        reg = NumpyRegister(SAVES_DIR, "hello")

        with reg.open() as reg:

            apri1 = ApriInfo(hi ="hello")
            apri2 = ApriInfo(num = 7, respective = apri1)

            reg.set_apos(apri1, AposInfo(no ="yes"))
            reg.add_disk_blk(Block(np.arange(10), apri2))

            for debug in [1,2,3]:

                cornifer.registers._debug = debug

                with self.assertRaises(KeyboardInterrupt):
                    reg.change_apri(apri1, ApriInfo(sup ="hey"), False)

                cornifer.registers._debug = _NO_DEBUG

                self.assertEqual(
                    AposInfo(no ="yes"),
                    reg.apos(ApriInfo(hi ="hello"))
                )

                self.assertTrue(np.all(
                    np.arange(10) ==
                    reg.blk(ApriInfo(num=7, respective=ApriInfo(hi="hello")), 0, 10).segment()
                ))

                self.assertIn(
                    ApriInfo(hi ="hello"),
                    reg
                )

                self.assertIn(
                    ApriInfo(num = 7, respective = ApriInfo(hi ="hello")),
                    reg
                )

                self.assertNotIn(
                    ApriInfo(sup ="hey"),
                    reg
                )

                self.assertNotIn(
                    ApriInfo(num = 7, respective = ApriInfo(sup ="hey")),
                    reg
                )

                get = reg.apris()

                self.assertEqual(
                    2,
                    len(get)
                )

                self.assertIn(
                    ApriInfo(hi ="hello"),
                    get
                )

                self.assertIn(
                    ApriInfo(num = 7, respective = ApriInfo(hi ="hello")),
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

        reg = NumpyRegister(SAVES_DIR, "hello")

        with self.assertRaisesRegex(RegisterError, "open.*concat_disk_blks"):
            reg.concat_disk_blks(ApriInfo(_ ="_"), 0, 0)

        with reg.open() as reg:

            apri = ApriInfo(hi ="hello")

            blk1 = Block(np.arange(100), apri)
            blk2 = Block(np.arange(100, 200), apri, 100)

            reg.add_disk_blk(blk1)
            reg.add_disk_blk(blk2)

            with self.assertRaisesRegex(ValueError, "right size"):
                reg.concat_disk_blks(apri, 0, 150, True)

            self.assertEqual(
                2,
                lmdb_count_keys(reg._db, _BLK_KEY_PREFIX)
            )

            self.assertEqual(
                2,
                lmdb_count_keys(reg._db, _COMPRESSED_KEY_PREFIX)
            )

            with self.assertRaisesRegex(ValueError, "right size"):
                reg.concat_disk_blks(apri, 1, 200)

            with self.assertRaisesRegex(ValueError, "right size"):
                reg.concat_disk_blks(apri, 0, 199)

            self.assertEqual(
                2,
                lmdb_count_keys(reg._db, _BLK_KEY_PREFIX)
            )

            self.assertEqual(
                2,
                lmdb_count_keys(reg._db, _COMPRESSED_KEY_PREFIX)
            )

            try:
                reg.concat_disk_blks(apri, 0, 200, True)

            except:
                self.fail("concat_disk_blks call should have succeeded")

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
                reg.blk(apri, 0, 200)
            )

            self.assertEqual(
                Block(np.arange(200), apri),
                reg.blk(apri)
            )

            try:
                # this shouldn't do anything
                reg.concat_disk_blks(apri)

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
                reg.blk(apri, 0, 200)
            )

            self.assertEqual(
                Block(np.arange(200), apri),
                reg.blk(apri)
            )

            blk3 = Block(np.arange(200, 4000), apri, 200)

            reg.add_disk_blk(blk3)

            reg.concat_disk_blks(apri, delete = True)

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
                reg.blk(apri, 0, 4000)
            )

            self.assertEqual(
                Block(np.arange(4000), apri),
                reg.blk(apri)
            )

            blk4 = Block(np.arange(4001, 4005), apri, 4001)

            reg.add_disk_blk(blk4)

            # this shouldn't do anything
            reg.concat_disk_blks(apri)

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
                reg.blk(apri, 0, 4000)
            )

            self.assertEqual(
                Block(np.arange(4000), apri),
                reg.blk(apri)
            )

            with self.assertRaisesRegex(DataNotFoundError, "4000"):
                reg.concat_disk_blks(apri, 0, 4001)

            with self.assertRaisesRegex(DataNotFoundError, "4000.*4000"):
                reg.concat_disk_blks(apri, 0, 4005)

            blk5 = Block(np.arange(3999, 4001), apri, 3999)

            reg.add_disk_blk(blk5)

            with self.assertRaisesRegex(ValueError, "[oO]verlap"):
                reg.concat_disk_blks(apri, 0, 4001)

            blk6 = Block(np.arange(4005, 4100), apri, 4005)
            blk7 = Block(np.arange(4100, 4200), apri, 4100)
            blk8 = Block(np.arange(4200, 4201), apri, 4200)

            reg.add_disk_blk(blk6)
            reg.add_disk_blk(blk7)
            reg.add_disk_blk(blk8)

            reg.concat_disk_blks(apri, 4005, delete = True)

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
                reg.blk(apri, 4005, 4201 - 4005)
            )

            self.assertEqual(
                Block(np.arange(4005, 4201), apri, 4005),
                reg.blk(apri, 4005)
            )

            self.assertEqual(
                Block(np.arange(4000), apri),
                reg.blk(apri)
            )

            blk9 = Block(np.arange(4201, 4201), apri, 4201)
            reg.add_disk_blk(blk9)

            reg.concat_disk_blks(apri, 4005, delete = True)

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
                reg.blk(apri, 4005, 4201 - 4005)
            )

            self.assertEqual(
                Block(np.arange(4005, 4201), apri, 4005),
                reg.blk(apri, 4005)
            )

            self.assertEqual(
                Block(np.arange(4000), apri),
                reg.blk(apri)
            )

            self.assertEqual(
                Block(np.arange(4201, 4201), apri, 4201),
                reg.blk(apri, 4201, 0)
            )

            blk10 = Block(np.arange(0, 0), apri, 0)
            reg.add_disk_blk(blk10)

            reg.rmv_disk_blk(apri, 3999, 2)

            reg.concat_disk_blks(apri, delete = True)

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
                reg.blk(apri, 4005, 4201 - 4005)
            )

            self.assertEqual(
                Block(np.arange(4005, 4201), apri, 4005),
                reg.blk(apri, 4005)
            )

            self.assertEqual(
                Block(np.arange(4000), apri),
                reg.blk(apri)
            )

            self.assertEqual(
                Block(np.arange(4201, 4201), apri, 4201),
                reg.blk(apri, 4201, 0)
            )

            self.assertEqual(
                Block(np.arange(0, 0), apri, 0),
                reg.blk(apri, 0, 0)
            )


        with reg.open(readonly= True) as reg:
            with self.assertRaisesRegex(RegisterError, "[rR]ead-write"):
                reg.concat_disk_blks(ApriInfo(_="_"), 0, 0)

    def _composite_helper(self, reg, block_datas, apris):

        with reg._db.begin() as txn:

            # check blocks
            for data, (seg, compressed) in block_datas.items():

                try:
                    filename = (txn
                                .get(reg._get_disk_blk_key(_BLK_KEY_PREFIX, data[0], None, data[1], data[2], False))
                                .decode("ASCII")
                    )
                except:
                    raise

                filename = reg._local_dir / filename

                self.assertTrue(filename.is_file())

                val = txn.get(reg._get_disk_blk_key(_COMPRESSED_KEY_PREFIX, data[0], None, data[1], data[2], False))

                self.assertEqual(
                    compressed,
                    val != _IS_NOT_COMPRESSED_VAL
                )

                if val == _IS_NOT_COMPRESSED_VAL:

                    self.assertEqual(
                        Block(seg, *data[:2]),
                        reg.blk(*data)
                    )

                else:

                    with self.assertRaises(CompressionError):
                        reg.blk(*data)

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
                reg.num_blks(apri)
            )


        # check info
        all_apri = reg.apris()

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
                    reg._local_dir / REG_FILENAME,
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
        # set msg
        # remove some data
        # combine disk blocks
        # compress it
        # set_startn_info
        # increase register size
        # move Register to a different savesDir
        # change info info
        # compress one at a time
        # decompress half
        # combine disk blocks
        # increase register size
        # change info info back

        block_datas = {}
        apris = []

        reg = NumpyRegister(SAVES_DIR, "hello")

        with reg.open() as reg:

            inner_apri = ApriInfo(descr ="\\\\hello", num = 7)
            apri = ApriInfo(descr ="\\'hi\"", respective = inner_apri)
            apris.append(inner_apri)
            apris.append(apri)
            seg = np.arange(69, 420)
            blk = Block(seg, apri, 1337)
            reg.add_disk_blk(blk)
            block_datas[data(blk)] = [seg, False]

            self._composite_helper(reg, block_datas, apris)

            seg = np.arange(69, 69)
            blk = Block(seg, apri, 1337)
            reg.add_disk_blk(blk)
            block_datas[data(blk)] = [seg, False]

            self._composite_helper(reg, block_datas, apris)

            apri = ApriInfo(descr ="Apri_Info.fromJson(hi = \"lol\")", respective = inner_apri)
            apris.append(apri)
            seg = np.arange(69., 420.)
            blk = Block(seg, apri, 1337)
            reg.add_disk_blk(blk)
            block_datas[data(blk)] = [seg, False]

            self._composite_helper(reg, block_datas, apris)

            for start_n, length in reg.intervals(
                    ApriInfo(descr="Apri_Info.fromJson(hi = \"lol\")", respective=inner_apri)):
                reg.compress(ApriInfo(descr ="Apri_Info.fromJson(hi = \"lol\")", respective = inner_apri), start_n, length)

            _set_block_datas_compressed(block_datas,
                ApriInfo(descr ="Apri_Info.fromJson(hi = \"lol\")", respective = inner_apri)
            )

            self._composite_helper(reg, block_datas, apris)

            for start_n, length in reg.intervals(ApriInfo(descr="\\'hi\"", respective=inner_apri)):
                reg.compress(ApriInfo(descr ="\\'hi\"", respective = inner_apri), start_n, length)

            _set_block_datas_compressed(block_datas,
                ApriInfo(descr ="\\'hi\"", respective = inner_apri)
            )

            self._composite_helper(reg, block_datas, apris)

            reg.rmv_disk_blk(
                ApriInfo(descr="\\'hi\"", respective=inner_apri)
            )

            del block_datas[ApriInfo(descr="\\'hi\"", respective=inner_apri), 1337, 420 - 69]

            self._composite_helper(reg, block_datas, apris)

            with self.assertRaisesRegex(ValueError, "`Block`"):
                reg.rmv_apri(ApriInfo(descr="\\'hi\"", respective=inner_apri))

            reg.rmv_disk_blk(
                ApriInfo(descr="\\'hi\"", respective=inner_apri)
            )

            del block_datas[ApriInfo(descr="\\'hi\"", respective=inner_apri), 1337, 0]

            self._composite_helper(reg, block_datas, apris)

            reg.rmv_apri(ApriInfo(descr="\\'hi\"", respective=inner_apri))

            del apris[apris.index(ApriInfo(descr="\\'hi\"", respective=inner_apri))]

            self._composite_helper(reg, block_datas, apris)

            with self.assertRaises(ValueError):
                reg.rmv_apri(inner_apri)

            reg.decompress(
                ApriInfo(descr ="Apri_Info.fromJson(hi = \"lol\")", respective = inner_apri),
                1337,
                420 - 69
            )

            _set_block_datas_compressed(
                block_datas,
                ApriInfo(descr ="Apri_Info.fromJson(hi = \"lol\")", respective = inner_apri),
                compressed = False
            )

            self._composite_helper(reg, block_datas, apris)

            new_message = "\\\\new msg\"\"\\'"
            reg.set_msg(new_message)

            self.assertEqual(
                new_message,
                str(reg)
            )

        self.assertEqual(
            new_message,
            str(reg)
        )

        reg = load(reg._local_dir)

        with reg.open() as reg:

            inner_inner_apri = ApriInfo(inner_apri = inner_apri)
            apri = ApriInfo(inner_apri = inner_inner_apri, love ="Apos_Info(num = 6)")
            apris.append(apri)
            apris.append(inner_inner_apri)

            datas = [(10, 34), (10 + 34, 8832), (10 + 34 + 8832, 0), (10 + 34 + 8832, 54), (10 + 34 + 8832 + 54, 0)]

            for start_n, length in datas:

                seg = np.arange(length, 2 * length)
                blk = Block(seg, apri, start_n)
                reg.add_disk_blk(blk)
                block_datas[data(blk)] = [seg, False]

                self._composite_helper(reg, block_datas, apris)

            with self.assertRaisesRegex(ValueError, re.escape(str(apri))):
                reg.rmv_apri(inner_inner_apri)

            reg.concat_disk_blks(apri, delete = True)

            for _data in datas:
                if _data[1] != 0:
                    del block_datas[(apri,) + _data]

            block_datas[(apri, datas[0][0], sum(length for _, length in datas))] = [
                np.concatenate([np.arange(length, 2*length) for _, length in datas]),
                False
            ]

            self._composite_helper(reg, block_datas, apris)

            reg.concat_disk_blks(apri, delete = True)

            self._composite_helper(reg, block_datas, apris)

            reg.compress(apri)

            block_datas[(apri, datas[0][0], sum(length for _, length in datas))][1] = True

            self._composite_helper(reg, block_datas, apris)

            for apri in reg:

                for start_n, length in reg.intervals(apri):
                    reg.rmv_disk_blk(apri, start_n, length)

            block_datas = {}

            self._composite_helper(reg, block_datas, apris)

            reg.set_startn_info(10 ** 13, 4)

            start_n = 10 ** 17

            for i in range(5):

                apri = ApriInfo(longg ="boi")
                blk = Block(np.arange(start_n + i*1000, start_n + (i+1)*1000, dtype = np.int64), apri, start_n + i*1000)
                reg.add_disk_blk(blk)

            with self.assertRaisesRegex(IndexError, "head"):
                reg.add_disk_blk(Block([], apri))

            for start_n, length in reg.intervals(apri):
                reg.rmv_disk_blk(apri, start_n, length)

            reg.set_startn_info()

            reg.increase_reg_size(reg.reg_size() + 1)

            with self.assertRaises(ValueError):
                reg.increase_reg_size(reg.reg_size() - 1)

    def test_remove_apri_info(self):

        reg = NumpyRegister(SAVES_DIR, "sup")

        with self.assertRaisesRegex(RegisterError, "open.*rmv_apri"):
            reg.rmv_apri(ApriInfo(no ="yes"))

        with reg.open() as reg:

            apri1 = ApriInfo(hello ="hi")
            apri2 = ApriInfo(sup ="hey")
            apri3 = ApriInfo(respective = apri1)

            reg.add_disk_blk(Block(np.arange(15), apri1))
            reg.set_apos(apri2, AposInfo(num = 7))
            reg.add_disk_blk(Block(np.arange(15, 30), apri3, 15))

            for i in [1,2,3]:

                apri = eval(f"apri{i}")

                with self.assertRaises(ValueError):
                    reg.rmv_apri(apri)

                get = reg.apris()

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

            try:
                reg.rmv_disk_blk(apri1, 0, 15)
            except DataNotFoundError:
                raise

            for i in [1,2,3]:

                apri = eval(f"apri{i}")

                with self.assertRaises(ValueError):
                    reg.rmv_apri(apri)

                get = reg.apris()

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

            reg.rmv_apos(apri2)

            for debug in [1,2,3,4]:

                cornifer.registers._debug = debug

                with self.assertRaises(KeyboardInterrupt):
                    reg.rmv_apri(apri2)

                cornifer.registers._debug = _NO_DEBUG

                for i in [1, 3]:

                    apri = eval(f"apri{i}")

                    with self.assertRaises(ValueError):
                        reg.rmv_apri(apri)

                    get = reg.apris()

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

            reg.rmv_apri(apri2)

            for i in [1,3]:

                apri = eval(f"apri{i}")

                with self.assertRaises(ValueError):
                    reg.rmv_apri(apri)

                get = reg.apris()

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

            reg.rmv_disk_blk(apri3, 15, 15)

            reg.rmv_apri(apri3)

            get = reg.apris()

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

            reg.rmv_apri(apri1)

            self.assertEqual(
                0,
                len(reg.apris())
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

        with self.assertRaisesRegex(RegisterError, "read-write"):
            with reg.open(readonly= True) as reg:
                reg.rmv_apri(ApriInfo(no ="yes"))

def _set_block_datas_compressed(block_datas, apri, start_n = None, length = None, compressed = True):

    for (_apri, _start_n, _length), val in block_datas.items():

        if _apri == apri and (start_n is None or _start_n == start_n) and (length is None or _length == length):

            val[1] = compressed