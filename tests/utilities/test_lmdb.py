import shutil
import unittest
from pathlib import Path

import lmdb

from cornifer._utilities import random_unique_filename, BYTES_PER_MB, BYTES_PER_KB, BYTES_PER_GB
from cornifer._utilities.lmdb import create_lmdb, open_lmdb, r_txn_prefix_iter, approx_memory

key = 'key'.encode('ASCII')
one = '1'.encode('ASCII')
empty = ''.encode('ASCII')

class TestLmdb(unittest.TestCase):

    def setUp(self):

        self.test_dir = random_unique_filename(Path.home())
        self.test_dir.mkdir(exist_ok = False)

    def tearDown(self):

        if not self.test_dir.exists():
            shutil.rmtree(self.test_dir)

    def stress(self, db, mapsize):

        with self.assertRaises(lmdb.MapFullError):

            with db.begin(write = True) as rw_txn:
                rw_txn.put(key, one * mapsize)

        with self.assertRaises(lmdb.MapFullError):

            for i in range(mapsize // BYTES_PER_KB):

                with db.begin(write = True) as rw_txn:
                    rw_txn.put(str(i).encode('ASCII'), one * BYTES_PER_KB)

        self.assertGreaterEqual(i, mapsize // BYTES_PER_KB // 2)

    def test_create_lmdb(self):

        for mapsize in (-1, 0, BYTES_PER_KB, 5 * BYTES_PER_KB, BYTES_PER_MB, 5 * BYTES_PER_MB, 10 * BYTES_PER_MB, 50 * BYTES_PER_MB):

            for max_readers in (-1, 0, 1, 10, 100):

                print(mapsize, max_readers)
                dir_ = random_unique_filename(self.test_dir)
                dir_.mkdir()

                if mapsize <= 0 or max_readers <= 0:

                    with self.assertRaises(ValueError):
                        create_lmdb(dir_, mapsize, max_readers)

                else:

                    db = create_lmdb(dir_, mapsize, max_readers)
                    self.assertTrue((dir_ / 'lock.mdb').exists())
                    self.assertTrue((dir_ / 'lock.mdb').is_file())
                    self.assertTrue((dir_ / 'data.mdb').exists())
                    self.assertTrue((dir_ / 'data.mdb').is_file())

                    if mapsize >= BYTES_PER_MB:
                        self.stress(db, mapsize)

    def test_open_lmdb(self):

        for mapsize in (BYTES_PER_MB, 5 * BYTES_PER_MB, 10 * BYTES_PER_MB, 50 * BYTES_PER_MB):

            for max_readers in (1, 10, 100):

                dir_ = random_unique_filename(self.test_dir)
                dir_.mkdir()
                db = create_lmdb(dir_, mapsize, max_readers)
                db.close()
                db = open_lmdb(dir_, False)
                self.stress(db, mapsize)