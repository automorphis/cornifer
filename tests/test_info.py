import json
from copy import copy
from unittest import TestCase

from cornifer import ApriInfo


class Test__Info(TestCase):

    def test___init__(self):

        with self.assertRaises(ValueError):
            ApriInfo()

        with self.assertRaises(ValueError):
            ApriInfo(_json ="sup")

        with self.assertRaises(ValueError):
            ApriInfo(_hash ="sup")

        with self.assertRaises(ValueError):
            ApriInfo(lst = [1, 2, 3])

        with self.assertRaises(ValueError):
            ApriInfo(dct = {1:2})

        try:
            ApriInfo(tup = (1, 2))
        except ValueError:
            self.fail("tuples are hashable")

        try:
            ApriInfo(msg ="hey")
        except ValueError:
            self.fail("strings are hashable")

        try:
            ApriInfo(pi ="π")
        except ValueError:
            self.fail("pi is okay")

        try:
            ApriInfo(double_null ="\0\0")
        except ValueError:
            self.fail("double null okay")

        apri = ApriInfo(msg ="primes", mod4 = 1)
        self.assertEqual(apri.msg, "primes")
        self.assertEqual(apri.mod4, 1)

    def test__from_json(self):

        with self.assertRaises(ValueError):
            ApriInfo.fromJson("[\"no\"]")

        apri = ApriInfo.fromJson("{\"msg\": \"primes\"}")
        self.assertEqual(apri.msg, "primes")

        apri = ApriInfo.fromJson("{\"mod4\": 1}")
        self.assertEqual(apri.mod4, 1)

        apri = ApriInfo.fromJson("{\"tup\": [1,2,3]}")
        self.assertEqual(apri.tup, (1,2,3))

        apri = ApriInfo.fromJson("""{"msg" : "primes", "respective" : "ApriInfo.fromJson({\\"haha\\" : \\"lol\\"})" }""")
        self.assertEqual(apri.msg, "primes")
        self.assertEqual(apri.respective, ApriInfo(haha ="lol"))

    def test__to_json(self):

        _json = ApriInfo(msg ="primes", mod4 = 3).toJson()
        self.assertTrue(isinstance(_json, str))
        obj = json.loads(_json)
        self.assertTrue(isinstance(obj, dict))
        self.assertEqual(len(obj), 2)
        self.assertEqual(obj, {"msg": "primes", "mod4": 3})

        _json = ApriInfo(msg="primes", primes = (2, 3, 5)).toJson()
        self.assertTrue(isinstance(_json, str))
        obj = json.loads(_json)
        self.assertTrue(isinstance(obj, dict))
        self.assertEqual(len(obj), 2)
        self.assertEqual(obj, {"msg": "primes", "primes": [2,3,5]})

        apri = ApriInfo(msg ="primes", primes = (2, 3, 5), respective = ApriInfo(lol ="haha"))
        self.assertEqual(apri, ApriInfo.fromJson(apri.toJson()))

    def test___hash__(self):

        self.assertEqual(
            hash(ApriInfo(msg ="primes", mod4 = 1)),
            hash(ApriInfo(mod4 = 1, msg ="primes"))
        )

        self.assertNotEqual(
            hash(ApriInfo(msg ="primes", mod4 = 1)),
            hash(ApriInfo(mod4 = 1))
        )

    def test___eq__(self):

        self.assertEqual(
            ApriInfo(msg ="primes", mod4 = 1),
            ApriInfo(mod4 = 1, msg ="primes")
        )

        self.assertNotEqual(
            ApriInfo(msg ="primes", mod4 = 1),
            ApriInfo(mod4 = 1)
        )

        self.assertNotEqual(
            ApriInfo(mod4 = 1),
            ApriInfo(msg ="primes", mod4 = 1)
        )

        self.assertEqual(
            ApriInfo(msg ="primes", respective = ApriInfo(hello ="hi", num = 7)),
            ApriInfo(respective = ApriInfo(num = 7, hello ="hi"), msg ="primes")
        )

        self.assertNotEqual(
            ApriInfo(msg ="primes", respective = ApriInfo(hello ="hi", num = 8)),
            ApriInfo(respective = ApriInfo(num = 7, hello ="hi"), msg ="primes")
        )

    def test___copy__(self):
        self.assertEqual(
            ApriInfo(no ="no"),
            copy(ApriInfo(no ="no"))
        )
        apri = ApriInfo(msg ="primes")
        self.assertEqual(
            apri,
            copy(apri)
        )
        self.assertEqual(
            hash(apri),
            hash(copy(apri))
        )
        apri = ApriInfo(msg ="primes", mod4 = 1)
        self.assertEqual(
            apri,
            copy(apri)
        )
        self.assertEqual(
            hash(apri),
            hash(copy(apri))
        )

    def test_iter_inner_info(self):

        apri = ApriInfo(descr ="descr")

        self.assertEqual(
            {(None, ApriInfo(descr ="descr"))},
            set(apri.iterInnerInfo())
        )

        self.assertEqual(
            1,
            sum(1 for _ in apri.iterInnerInfo())
        )

        apri = ApriInfo(descr = ApriInfo(num = 7))

        self.assertEqual(
            {
                (None, ApriInfo(descr = ApriInfo(num = 7))),
                ("descr", ApriInfo(num = 7))
            },
            set(apri.iterInnerInfo())
        )

        self.assertEqual(
            2,
            sum(1 for _ in apri.iterInnerInfo())
        )

        apri = ApriInfo(descr = ApriInfo(blub = ApriInfo(hi ="hello")))

        self.assertEqual(
            {
                (None, ApriInfo(descr = ApriInfo(blub = ApriInfo(hi ="hello")))),
                ("descr", ApriInfo(blub = ApriInfo(hi ="hello"))),
                ("blub", ApriInfo(hi ="hello"))
            },
            set(apri.iterInnerInfo())
        )

        self.assertEqual(
            3,
            sum(1 for _ in apri.iterInnerInfo())
        )

        apri = ApriInfo(num = 7, descr = ApriInfo(blub = ApriInfo(hi ="hello")))

        self.assertEqual(
            {
                (None, ApriInfo(num = 7, descr = ApriInfo(blub = ApriInfo(hi ="hello")))),
                ("descr", ApriInfo(blub = ApriInfo(hi ="hello"))),
                ("blub", ApriInfo(hi ="hello"))
            },
            set(apri.iterInnerInfo())
        )

        self.assertEqual(
            3,
            sum(1 for _ in apri.iterInnerInfo())
        )

        apri = ApriInfo(num = 7, descr = ApriInfo(no ="yes", blub = ApriInfo(hi ="hello")))

        self.assertEqual(
            {
                (None, ApriInfo(num = 7, descr = ApriInfo(no ="yes", blub = ApriInfo(hi ="hello")))),
                ("descr", ApriInfo(no ="yes", blub = ApriInfo(hi ="hello"))),
                ("blub", ApriInfo(hi ="hello"))
            },
            set(apri.iterInnerInfo())
        )

        self.assertEqual(
            3,
            sum(1 for _ in apri.iterInnerInfo())
        )

        apri = ApriInfo(num = ApriInfo(descr ="hi"), two = ApriInfo(descr ="hi"))

        self.assertEqual(
            {
                (None, ApriInfo(num = ApriInfo(descr ="hi"), two = ApriInfo(descr ="hi"))),
                ("num", ApriInfo(descr ="hi")),
                ("two", ApriInfo(descr ="hi"))
            },
            set(apri.iterInnerInfo())
        )

        self.assertEqual(
            3,
            sum(1 for _ in apri.iterInnerInfo())
        )

        apri = ApriInfo(num = ApriInfo(descr ="hey"), two = ApriInfo(descr ="hi"))

        self.assertEqual(
            {
                (None, ApriInfo(num = ApriInfo(descr ="hey"), two = ApriInfo(descr ="hi"))),
                ("num", ApriInfo(descr ="hey")),
                ("two", ApriInfo(descr ="hi"))
            },
            set(apri.iterInnerInfo())
        )

        self.assertEqual(
            3,
            sum(1 for _ in apri.iterInnerInfo())
        )

    def test_change_info(self):

        apri = ApriInfo(descr ="descr")

        with self.assertRaises(TypeError):
            apri.changeInfo(apri, 0)

        with self.assertRaises(TypeError):
            apri.changeInfo(0, apri)

        replaced = apri.changeInfo(ApriInfo(no ="yes"), ApriInfo(maybe ="maybe"))

        self.assertEqual(
            ApriInfo(descr ="descr"),
            replaced
        )

        replaced = apri.changeInfo(apri, ApriInfo(no ="yes"))

        self.assertEqual(
            ApriInfo(no ="yes"),
            replaced
        )

        apri = ApriInfo(descr = ApriInfo(num = 7))

        replaced = apri.changeInfo(ApriInfo(num = 7), ApriInfo(_num = 8))

        self.assertEqual(
            ApriInfo(descr = ApriInfo(_num = 8)),
            replaced
        )

        replaced = apri.changeInfo(apri, ApriInfo(hello ="hi"))

        self.assertEqual(
            ApriInfo(hello ="hi"),
            replaced
        )

        apri = ApriInfo(descr = ApriInfo(blub = ApriInfo(hi ="hello")))

        replaced = apri.changeInfo(ApriInfo(hi ="hello"), ApriInfo(hi ="hellox"))

        self.assertEqual(
            ApriInfo(descr = ApriInfo(blub = ApriInfo(hi ="hellox"))),
            replaced
        )

        replaced = apri.changeInfo(ApriInfo(blub = ApriInfo(hi ="hello")), ApriInfo(bloob = ApriInfo(hi ="hello")))

        self.assertEqual(
            ApriInfo(descr = ApriInfo(bloob = ApriInfo(hi ="hello"))),
            replaced
        )

        replaced = apri.changeInfo(ApriInfo(descr = ApriInfo(blub = ApriInfo(hi ="hello"))), ApriInfo(descr ="yes"))

        self.assertEqual(
            ApriInfo(descr ="yes"),
            replaced
        )

        apri = ApriInfo(num = 7, descr = ApriInfo(blub = ApriInfo(hi ="hello")))

        replaced = apri.changeInfo(ApriInfo(blub = ApriInfo(hi ="hello")), ApriInfo(bloob = ApriInfo(hi ="hello")))

        self.assertEqual(
            ApriInfo(num = 7, descr = ApriInfo(bloob = ApriInfo(hi ="hello"))),
            replaced
        )

        replaced = apri.changeInfo(ApriInfo(descr = ApriInfo(blub = ApriInfo(hi ="hello"))), ApriInfo(descr ="yes"))

        self.assertEqual(
            ApriInfo(num = 7, descr = ApriInfo(blub = ApriInfo(hi ="hello"))),
            replaced
        )

        replaced = apri.changeInfo(ApriInfo(num = 7, descr = ApriInfo(blub = ApriInfo(hi ="hello"))), ApriInfo(loot ="chest"))

        self.assertEqual(
            ApriInfo(loot ="chest"),
            replaced
        )

        apri = ApriInfo(num = ApriInfo(descr ="hi"), two = ApriInfo(descr ="hi"))

        replaced = apri.changeInfo(ApriInfo(descr ="hi"), ApriInfo(num = ApriInfo(descr ="hi")))

        self.assertEqual(
            ApriInfo(num = ApriInfo(num = ApriInfo(descr ="hi")), two = ApriInfo(num = ApriInfo(descr ="hi"))),
            replaced
        )

        apri = ApriInfo(num = ApriInfo(descr ="hey"), two = ApriInfo(descr ="hi"))

        replaced = apri.changeInfo(ApriInfo(descr ="hi"), ApriInfo(num = ApriInfo(descr ="hi")))

        self.assertEqual(
            ApriInfo(num = ApriInfo(descr ="hey"), two = ApriInfo(num = ApriInfo(descr ="hi"))),
            replaced
        )