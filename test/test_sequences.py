import json
import math
from copy import copy
from itertools import product
from unittest import TestCase

import numpy as np

from cornifer import Sequence_Description, Block
from cornifer.errors import Sequence_Description_Keyword_Argument_Error


class Test_Sequence_Description(TestCase):

    def test___init__(self):

        with self.assertRaises(Sequence_Description_Keyword_Argument_Error):
            Sequence_Description(_json = "sup")

        with self.assertRaises(Sequence_Description_Keyword_Argument_Error):
            Sequence_Description(_hash = "sup")

        with self.assertRaises(Sequence_Description_Keyword_Argument_Error):
            Sequence_Description(lst = [1,2,3])

        with self.assertRaises(Sequence_Description_Keyword_Argument_Error):
            Sequence_Description(dct = {1:2})

        try:
            Sequence_Description(tup = (1,2))
        except Sequence_Description_Keyword_Argument_Error:
            self.fail("tuples are hashable")

        try:
            Sequence_Description(msg = "hey")
        except Sequence_Description_Keyword_Argument_Error:
            self.fail("strings are hashable")

        try:
            Sequence_Description(pi = "π")
        except Sequence_Description_Keyword_Argument_Error:
            self.fail("pi is okay")

        try:
            Sequence_Description(double_null = "\0\0")
        except Sequence_Description_Keyword_Argument_Error:
            self.fail("double null okay")

        descr = Sequence_Description(msg = "primes", mod4 = 1)
        self.assertEqual(descr.msg, "primes")
        self.assertEqual(descr.mod4, 1)

    def test__from_json(self):
        with self.assertRaises(TypeError):
            Sequence_Description.from_json("[\"no\"]")
        descr = Sequence_Description.from_json("{\"msg\": \"primes\"}")
        self.assertEqual(descr.msg, "primes")
        descr = Sequence_Description.from_json("{\"mod4\": 1}")
        self.assertEqual(descr.mod4, 1)
        descr = Sequence_Description.from_json("{\"tup\": [1,2,3]}")
        self.assertEqual(descr.tup, (1,2,3))

    def test__to_json(self):
        _json = Sequence_Description(msg = "primes", mod4 = 3).to_json()
        self.assertTrue(isinstance(_json, str))
        obj = json.loads(_json)
        self.assertTrue(isinstance(obj, dict))
        self.assertEqual(len(obj), 2)
        self.assertEqual(obj, {"msg": "primes", "mod4": 3})

        _json = Sequence_Description(msg="primes", primes = (2,3,5)).to_json()
        self.assertTrue(isinstance(_json, str))
        obj = json.loads(_json)
        self.assertTrue(isinstance(obj, dict))
        self.assertEqual(len(obj), 2)
        self.assertEqual(obj, {"msg": "primes", "primes": [2,3,5]})

    def test___hash__(self):
        self.assertEqual(
            hash(Sequence_Description(msg = "primes", mod4 = 1)),
            hash(Sequence_Description(mod4 = 1, msg = "primes"))
        )
        self.assertNotEqual(
            hash(Sequence_Description(msg = "primes", mod4 = 1)),
            hash(Sequence_Description(mod4 = 1))
        )

    def test___eq__(self):
        self.assertEqual(
            Sequence_Description(msg = "primes", mod4 = 1),
            Sequence_Description(mod4 = 1, msg = "primes")
        )
        self.assertNotEqual(
            Sequence_Description(msg = "primes", mod4 = 1),
            Sequence_Description(mod4 = 1)
        )
        self.assertNotEqual(
            Sequence_Description(mod4 = 1),
            Sequence_Description(msg = "primes", mod4 = 1)
        )

    def test___copy__(self):
        self.assertEqual(
            Sequence_Description(),
            copy(Sequence_Description())
        )
        descr = Sequence_Description(msg = "primes")
        self.assertEqual(
            descr,
            copy(descr)
        )
        self.assertEqual(
            hash(descr),
            hash(copy(descr))
        )
        descr = Sequence_Description(msg = "primes", mod4 = 1)
        self.assertEqual(
            descr,
            copy(descr)
        )
        self.assertEqual(
            hash(descr),
            hash(copy(descr))
        )

class Test_Block(TestCase):

    def test___init__(self):

        descr = Sequence_Description(name = "primes")

        with self.assertRaises(TypeError):
            Block([], "primes", 0)

        class A:pass
        with self.assertRaises(ValueError):
            Block(A(), descr, 0)

        try:
            Block(np.array([]), descr, 0)
        except (ValueError, TypeError):
            self.fail("array is fine")

        try:
            Block([], descr, 0)
        except (ValueError, TypeError):
            self.fail("list is fine")

        class A:
            def __len__(self):pass
        try:
            Block(A(), descr, 0)
        except (ValueError, TypeError):
            self.fail("custom type is fine")

        with self.assertRaises(ValueError):
            Block([], descr, -1)

        with self.assertRaises(TypeError):
            Block([], descr, 0.5)

        self.assertEqual(
            Block([], descr).get_start_n(),
            0
        )

    def test_set_start_n(self):

        descr = Sequence_Description(name = "primes")

        seq = Block([], descr, 0)
        with self.assertRaises(TypeError):
            seq.set_start_n(0.5)

        seq = Block([], descr, 0)
        with self.assertRaises(ValueError):
            seq.set_start_n(-1)

        seq = Block([], descr)
        seq.set_start_n(15)
        self.assertEqual(
            seq.get_start_n(),
            15
        )

    def test_subdivide(self):
        descr = Sequence_Description(name = "primes")

        with self.assertRaises(TypeError):
            Block([], descr).subdivide(3.5)

        with self.assertRaises(ValueError):
            Block([], descr).subdivide(1)

        for length in [2, 3, 4, 5, 6, 7, 8, 9, 10, 27]:

            seqs = Block(np.arange(50), descr).subdivide(length)
            self.assertEqual(
                len(seqs),
                math.ceil(50 / length)
            )
            self.assertTrue(
                all(len(seq) == length for seq in seqs[:-1])
            )
            self.assertEqual(
                len(seqs[-1]),
                50 % length if 50 % length != 0 else length
            )

            seqs = Block(np.arange(50), descr, 1).subdivide(length)
            self.assertEqual(
                len(seqs),
                math.ceil(50 / length)
            )
            self.assertTrue(
                all(len(seq) == length for seq in seqs[:-1])
            )
            self.assertEqual(
                len(seqs[-1]),
                50 % length if 50 % length != 0 else length
            )

    def test___getitem__(self):
        descr = Sequence_Description(name="primes")

        with self.assertRaises(IndexError):
            Block(np.empty((50, 50)), descr)[25, 25]

        with self.assertRaises(IndexError):
            Block(np.empty(50), descr)[60]

        with self.assertRaises(IndexError):
            Block([0] * 50, descr)[60]

        self.assertEqual(
            Block(np.arange(50), descr)[:],
            Block(np.arange(50), descr)
        )

        self.assertEqual(
            Block(list(range(50)), descr)[:],
            Block(list(range(50)), descr)
        )

        self.assertNotEqual(
            Block(np.arange(50), descr)[:],
            Block(list(range(50)), descr)
        )

        self.assertNotEqual(
            Block(np.arange(50), descr),
            Block(list(range(50)), descr)[:]
        )

        self.assertEqual(
            Block(np.arange(50), descr)[0:],
            Block(np.arange(50), descr)
        )

        self.assertEqual(
            Block(list(range(50)), descr)[0:],
            Block(list(range(50)), descr)
        )

        self.assertNotEqual(
            Block(np.arange(50), descr)[0:],
            Block(list(range(50)), descr)
        )

        self.assertNotEqual(
            Block(np.arange(50), descr),
            Block(list(range(50)), descr)[0:]
        )

        self.assertEqual(
            Block(np.arange(50), descr)[:50],
            Block(np.arange(50), descr)
        )

        self.assertEqual(
            Block(list(range(50)), descr)[:50],
            Block(list(range(50)), descr)
        )

        self.assertNotEqual(
            Block(np.arange(50), descr)[:50],
            Block(list(range(50)), descr)
        )

        self.assertNotEqual(
            Block(np.arange(50), descr),
            Block(list(range(50)), descr)[:50]
        )

        self.assertEqual(
            Block(np.arange(50), descr)[0:50],
            Block(np.arange(50), descr)
        )

        self.assertEqual(
            Block(list(range(50)), descr)[0:50],
            Block(list(range(50)), descr)
        )

        self.assertNotEqual(
            Block(np.arange(50), descr)[0:50],
            Block(list(range(50)), descr)
        )

        self.assertNotEqual(
            Block(np.arange(50), descr),
            Block(list(range(50)), descr)[0:50]
        )

        self.assertEqual(
            Block(np.arange(50), descr)[:49],
            Block(np.arange(49), descr)
        )

        self.assertEqual(
            Block(np.arange(50), descr)[:-1],
            Block(np.arange(49), descr)
        )

        self.assertEqual(
            Block(list(range(50)), descr)[:49],
            Block(list(range(49)), descr)
        )

        self.assertEqual(
            Block(list(range(50)), descr)[:-1],
            Block(list(range(49)), descr)
        )

        self.assertEqual(
            Block(np.arange(50), descr)[1:],
            Block(np.arange(1, 50), descr)
        )

        self.assertEqual(
            Block(np.arange(50), descr, 1)[:],
            Block(np.arange(50), descr, 1)
        )

        self.assertEqual(
            Block(np.arange(50), descr, 1)[1:],
            Block(np.arange(50), descr, 1)
        )

        self.assertEqual(
            Block(np.arange(50), descr, 1)[2:],
            Block(np.arange(1, 50), descr, 1)
        )

        self.assertEqual(
            Block(np.arange(50), descr, 1)[:51],
            Block(np.arange(50), descr, 1)
        )

        self.assertEqual(
            Block(np.arange(50), descr, 1)[:-1],
            Block(np.arange(49), descr, 1)
        )

        self.assertEqual(
            Block(np.arange(0, 50), descr)[::3],
            Block(np.arange(0, 50, 3), descr)
        )

        self.assertEqual(
            Block(np.arange(0, 50), descr, 1)[::3],
            Block(np.arange(0, 50, 3), descr, 1)
        )

        self.assertEqual(
            Block(np.arange(0, 50), descr, 1)[1::3],
            Block(np.arange(0, 50, 3), descr, 1)
        )

        self.assertEqual(
            Block(np.arange(0, 50), descr, 1)[1::-3],
            Block(np.arange(0, 50, -3), descr, 1)
        )

        self.assertEqual(
            Block(np.arange(50), descr)[:1],
            Block(np.arange(1), descr)
        )

    def test___len__(self):
        descr = Sequence_Description(name = "primes")

        lst = []
        seq = Block(lst, descr)
        self.assertEqual(
            len(seq),
            0
        )

        lst.append("lol")
        self.assertEqual(
            len(seq),
            1
        )

        lst.extend(list(range(10)))
        self.assertEqual(
            len(seq),
            11
        )

        array = np.empty(10)
        seq = Block(array, descr)
        self.assertEqual(
            len(seq),
            10
        )

        array = np.empty(0)
        seq = Block(array, descr)
        self.assertEqual(
            len(seq),
            0
        )

        class A:
            def __len__(self): return 694201337

        seq = Block(A(), descr)
        self.assertEqual(
            len(seq),
            694201337
        )

    def test___contains__(self):
        descr = Sequence_Description(name = "primes")
        for n, start_n in product(range(50), repeat = 2):
            self.assertIn(
                n + start_n,
                Block(np.arange(50), descr, start_n)
            )

    def test___eq__(self):

        descr1 = Sequence_Description(name = "primes")
        descr2 = Sequence_Description(name = "primes", mod4 = 1)

        self.assertEqual(
            Block(np.arange(50), descr1),
            Block(np.arange(50), descr1)
        )

        self.assertEqual(
            Block(list(range(50)), descr1),
            Block(list(range(50)), descr1)
        )

        self.assertNotEqual(
            Block(np.arange(50), descr2),
            Block(np.arange(50), descr1)
        )

        self.assertNotEqual(
            Block(np.arange(60), descr1),
            Block(np.arange(50), descr1)
        )

        class Block2(Block):pass
        self.assertNotEqual(
            Block(np.arange(50), descr1),
            Block2(np.arange(50), descr1)
        )

        self.assertNotEqual(
            Block(np.arange(50), descr1, 0),
            Block(np.arange(50), descr1, 1)
        )

        self.assertNotEqual(
            Block(list(range(50)), descr1),
            Block(np.arange(50), descr1)
        )

    def test___hash__(self):

        with self.assertRaises(TypeError):
            hash(Block(np.arange(50), Sequence_Description(name = "primes")))