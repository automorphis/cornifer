import math
from itertools import product
from unittest import TestCase

import numpy as np

from cornifer import Block, openblks
from cornifer.info import ApriInfo


class Test_Block(TestCase):

    def test___init__(self):

        descr = ApriInfo(name ="primes")

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
            Block([], descr).startn(),
            0
        )

    def test_set_start_n(self):

        descr = ApriInfo(name ="primes")

        seq = Block([], descr, 0)
        with self.assertRaises(TypeError):
            seq.set_startn(0.5)

        seq = Block([], descr, 0)
        with self.assertRaises(ValueError):
            seq.set_startn(-1)

        seq = Block([], descr)
        seq.set_startn(15)
        self.assertEqual(
            seq.startn(),
            15
        )

    def test_subdivide(self):

        apri = ApriInfo(name ="primes")

        with self.assertRaises(TypeError):

            with Block([], apri) as blk:
                blk.subdivide(3.5)

        with self.assertRaises(ValueError):

            with Block([], apri) as blk:
                blk.subdivide(1)

        for length in [2, 3, 4, 5, 6, 7, 8, 9, 10, 27]:

            with Block(np.arange(50), apri) as blk:
                segs = blk.subdivide(length)

            self.assertEqual(
                len(segs),
                math.ceil(50 / length)
            )

            for seg in segs[:-1]:
                self.assertTrue(len(seg) == length)

            self.assertEqual(
                len(segs[-1]),
                50 % length if 50 % length != 0 else length
            )

            with Block(np.arange(50), apri, 1) as blk:
                segs = blk.subdivide(length)

            self.assertEqual(
                len(segs),
                math.ceil(50 / length)
            )

            for seg in segs[:-1]:
                self.assertTrue(len(seg) == length)

            self.assertEqual(
                len(segs[-1]),
                50 % length if 50 % length != 0 else length
            )

    def test___getitem__(self):

        apri = ApriInfo(name="primes")

        with self.assertRaises(IndexError):

            with Block(np.empty((50, 50)), apri) as blk:
                blk[25, 25]

        with self.assertRaises(IndexError):

            with Block(np.empty(50), apri) as blk:
                blk[60]

        with self.assertRaises(IndexError):

            with Block([0] * 50, apri) as blk:
                blk[60]

        with Block(np.arange(50), apri) as blk:
            self.assertTrue(np.all(blk[:] == np.arange(50)))

        with Block(list(range(50)), apri) as blk:
            self.assertEqual(blk[:], list(range(50)))

        with Block(np.arange(50), apri) as blk:
            self.assertTrue(np.all(blk[0:] == np.arange(50)))

        with Block(list(range(50)), apri) as blk:
            self.assertEqual(blk[0:], list(range(50)))

        with Block(np.arange(50), apri) as blk:
            self.assertTrue(np.all(blk[:50] == np.arange(50)))

        with Block(list(range(50)), apri) as blk:
            self.assertEqual(blk[:50], list(range(50)))

        with Block(np.arange(50), apri) as blk:
            self.assertTrue(np.all(blk[0:50] == np.arange(50)))

        with Block(list(range(50)), apri) as blk:
            self.assertEqual(blk[0:50], list(range(50)))

        with Block(np.arange(50), apri) as blk:
            self.assertTrue(np.all(blk[:49] == np.arange(49)))

        with Block(np.arange(50), apri) as blk:
            self.assertTrue(np.all(blk[:-1] == np.arange(49)))

        with Block(list(range(50)), apri) as blk:
            self.assertEqual(blk[:49], list(range(49)))

        with Block(list(range(50)), apri) as blk:
            self.assertEqual(blk[:-1], list(range(49)))

        with Block(np.arange(50), apri) as blk:
            self.assertTrue(np.all(blk[1:] == np.arange(1, 50)))

        with Block(np.arange(50), apri, 1) as blk:
            self.assertTrue(np.all(blk[:] == np.arange(50)))

        with Block(np.arange(50), apri, 1) as blk:
            self.assertTrue(np.all(blk[1:] == np.arange(50)))

        with Block(np.arange(50), apri, 1) as blk:
            self.assertTrue(np.all(blk[2:] == np.arange(1, 50)))

        with Block(np.arange(50), apri, 1) as blk:
            self.assertTrue(np.all(blk[:51] == np.arange(50)))

        with Block(np.arange(50), apri, 1) as blk:
            self.assertTrue(np.all(blk[:-1] == np.arange(49)))

        with Block(np.arange(50), apri) as blk:
            self.assertTrue(np.all(blk[::3] == np.arange(0, 50, 3)))

        with Block(np.arange(50), apri, 1) as blk:
            self.assertTrue(np.all(blk[::3] == np.arange(0, 50, 3)))

        with Block(np.arange(50), apri, 1) as blk:
            self.assertTrue(np.all(blk[1::3] == np.arange(0, 50, 3)))

        with Block(np.arange(50), apri, 1) as blk:
            self.assertTrue(np.all(blk[1::-3] == np.arange(0, 50, -3)))

        with Block(np.arange(50), apri) as blk:
            self.assertTrue(np.all(blk[:1] == np.arange(1)))

    def test___len__(self):

        apri = ApriInfo(name ="primes")
        seg = []
        blk = Block(seg, apri)

        with blk:
            self.assertEqual(
                len(blk),
                0
            )

        seg.append("lol")

        with blk:
            self.assertEqual(
                len(blk),
                1
            )

        seg.extend(list(range(10)))

        with blk:
            self.assertEqual(
                len(blk),
                11
            )

        array = np.empty(10)
        blk = Block(array, apri)

        with blk:
            self.assertEqual(
                len(blk),
                10
            )

        array = np.empty(0)
        blk = Block(array, apri)

        with blk:
            self.assertEqual(
                len(blk),
                0
            )

        class A:
            def __len__(self): return 694201337

        blk = Block(A(), apri)

        with blk:
            self.assertEqual(
                len(blk),
                694201337
            )

    def test___contains__(self):

        descr = ApriInfo(name ="primes")

        for n, start_n in product(range(50), repeat = 2):

            with Block(np.arange(50), descr, start_n) as blk:

                self.assertIn(
                    n + start_n,
                    blk
                )

    def test___eq__(self):

        descr1 = ApriInfo(name ="primes")
        descr2 = ApriInfo(name ="primes", mod4 = 1)

        with openblks(Block(np.arange(50), descr1), Block(np.arange(50), descr1)) as (blk1, blk2):
            self.assertEqual(blk1, blk2)

        with openblks(Block(list(range(50)), descr1), Block(list(range(50)), descr1)) as (blk1, blk2):
            self.assertEqual(blk1, blk2)

        with openblks(Block(np.arange(50), descr2), Block(np.arange(50), descr1)) as (blk1, blk2):
            self.assertNotEqual(blk1, blk2)

        with openblks(Block(np.arange(60), descr1), Block(np.arange(50), descr1)) as (blk1, blk2):
            self.assertNotEqual(blk1, blk2)

        class Block2(Block):pass

        with openblks(Block(np.arange(50), descr1), Block2(np.arange(50), descr1)) as (blk1, blk2):
            self.assertNotEqual(blk1, blk2)

        with openblks(Block(np.arange(50), descr1, 0), Block(np.arange(50), descr1, 1)) as (blk1, blk2):
            self.assertNotEqual(blk1, blk2)

        with openblks(Block(list(range(50)), descr1), Block(np.arange(50), descr1)) as (blk1, blk2):
            self.assertNotEqual(blk1, blk2)

    def test___hash__(self):

        with self.assertRaises(TypeError):
            hash(Block(np.arange(50), ApriInfo(name ="primes")))