import json
from copy import copy
from unittest import TestCase

from cornifer import Apri_Info


class Test__Info(TestCase):

    def test___init__(self):

        with self.assertRaises(ValueError):
            Apri_Info()

        with self.assertRaises(ValueError):
            Apri_Info(_json ="sup")

        with self.assertRaises(ValueError):
            Apri_Info(_hash ="sup")

        with self.assertRaises(ValueError):
            Apri_Info(lst = [1, 2, 3])

        with self.assertRaises(ValueError):
            Apri_Info(dct = {1:2})

        try:
            Apri_Info(tup = (1, 2))
        except ValueError:
            self.fail("tuples are hashable")

        try:
            Apri_Info(msg ="hey")
        except ValueError:
            self.fail("strings are hashable")

        try:
            Apri_Info(pi ="π")
        except ValueError:
            self.fail("pi is okay")

        try:
            Apri_Info(double_null ="\0\0")
        except ValueError:
            self.fail("double null okay")

        apri = Apri_Info(msg ="primes", mod4 = 1)
        self.assertEqual(apri.msg, "primes")
        self.assertEqual(apri.mod4, 1)

    def test__from_json(self):

        with self.assertRaises(ValueError):
            Apri_Info.from_json("[\"no\"]")

        apri = Apri_Info.from_json("{\"msg\": \"primes\"}")
        self.assertEqual(apri.msg, "primes")

        apri = Apri_Info.from_json("{\"mod4\": 1}")
        self.assertEqual(apri.mod4, 1)

        apri = Apri_Info.from_json("{\"tup\": [1,2,3]}")
        self.assertEqual(apri.tup, (1,2,3))

        apri = Apri_Info.from_json("""{"msg" : "primes", "respective" : "Apri_Info.from_json({\\"haha\\" : \\"lol\\"})" }""")
        self.assertEqual(apri.msg, "primes")
        self.assertEqual(apri.respective, Apri_Info(haha = "lol"))

    def test__to_json(self):

        _json = Apri_Info(msg ="primes", mod4 = 3).to_json()
        self.assertTrue(isinstance(_json, str))
        obj = json.loads(_json)
        self.assertTrue(isinstance(obj, dict))
        self.assertEqual(len(obj), 2)
        self.assertEqual(obj, {"msg": "primes", "mod4": 3})

        _json = Apri_Info(msg="primes", primes = (2, 3, 5)).to_json()
        self.assertTrue(isinstance(_json, str))
        obj = json.loads(_json)
        self.assertTrue(isinstance(obj, dict))
        self.assertEqual(len(obj), 2)
        self.assertEqual(obj, {"msg": "primes", "primes": [2,3,5]})

        apri = Apri_Info(msg = "primes", primes = (2,3,5), respective = Apri_Info(lol = "haha"))
        self.assertEqual(apri, Apri_Info.from_json(apri.to_json()))

    def test___hash__(self):

        self.assertEqual(
            hash(Apri_Info(msg ="primes", mod4 = 1)),
            hash(Apri_Info(mod4 = 1, msg ="primes"))
        )

        self.assertNotEqual(
            hash(Apri_Info(msg ="primes", mod4 = 1)),
            hash(Apri_Info(mod4 = 1))
        )

    def test___eq__(self):

        self.assertEqual(
            Apri_Info(msg ="primes", mod4 = 1),
            Apri_Info(mod4 = 1, msg ="primes")
        )

        self.assertNotEqual(
            Apri_Info(msg ="primes", mod4 = 1),
            Apri_Info(mod4 = 1)
        )

        self.assertNotEqual(
            Apri_Info(mod4 = 1),
            Apri_Info(msg ="primes", mod4 = 1)
        )

        self.assertEqual(
            Apri_Info(msg = "primes", respective = Apri_Info(hello = "hi", num = 7)),
            Apri_Info(respective = Apri_Info(num = 7, hello = "hi"), msg = "primes")
        )

        self.assertNotEqual(
            Apri_Info(msg = "primes", respective = Apri_Info(hello = "hi", num = 8)),
            Apri_Info(respective = Apri_Info(num = 7, hello = "hi"), msg = "primes")
        )

    def test___copy__(self):
        self.assertEqual(
            Apri_Info(no = "no"),
            copy(Apri_Info(no = "no"))
        )
        apri = Apri_Info(msg ="primes")
        self.assertEqual(
            apri,
            copy(apri)
        )
        self.assertEqual(
            hash(apri),
            hash(copy(apri))
        )
        apri = Apri_Info(msg ="primes", mod4 = 1)
        self.assertEqual(
            apri,
            copy(apri)
        )
        self.assertEqual(
            hash(apri),
            hash(copy(apri))
        )

    def test_iter_inner_info(self):

        apri = Apri_Info(descr = "descr")

        self.assertEqual(
            {(None, Apri_Info(descr = "descr"))},
            set(apri.iter_inner_info())
        )

        self.assertEqual(
            1,
            sum(1 for _ in apri.iter_inner_info())
        )

        apri = Apri_Info(descr = Apri_Info(num = 7))

        self.assertEqual(
            {
                (None, Apri_Info(descr = Apri_Info(num = 7))),
                ("descr", Apri_Info(num = 7))
            },
            set(apri.iter_inner_info())
        )

        self.assertEqual(
            2,
            sum(1 for _ in apri.iter_inner_info())
        )

        apri = Apri_Info(descr = Apri_Info(blub = Apri_Info(hi = "hello")))

        self.assertEqual(
            {
                (None, Apri_Info(descr = Apri_Info(blub = Apri_Info(hi = "hello")))),
                ("descr", Apri_Info(blub = Apri_Info(hi = "hello"))),
                ("blub", Apri_Info(hi = "hello"))
            },
            set(apri.iter_inner_info())
        )

        self.assertEqual(
            3,
            sum(1 for _ in apri.iter_inner_info())
        )

        apri = Apri_Info(num = 7, descr = Apri_Info(blub = Apri_Info(hi = "hello")))

        self.assertEqual(
            {
                (None, Apri_Info(num = 7, descr = Apri_Info(blub = Apri_Info(hi = "hello")))),
                ("descr", Apri_Info(blub = Apri_Info(hi = "hello"))),
                ("blub", Apri_Info(hi = "hello"))
            },
            set(apri.iter_inner_info())
        )

        self.assertEqual(
            3,
            sum(1 for _ in apri.iter_inner_info())
        )

        apri = Apri_Info(num = 7, descr = Apri_Info(no = "yes", blub = Apri_Info(hi = "hello")))

        self.assertEqual(
            {
                (None, Apri_Info(num = 7, descr = Apri_Info(no = "yes", blub = Apri_Info(hi = "hello")))),
                ("descr", Apri_Info(no = "yes", blub = Apri_Info(hi = "hello"))),
                ("blub", Apri_Info(hi = "hello"))
            },
            set(apri.iter_inner_info())
        )

        self.assertEqual(
            3,
            sum(1 for _ in apri.iter_inner_info())
        )

        apri = Apri_Info(num = Apri_Info(descr = "hi"), two = Apri_Info(descr = "hi"))

        self.assertEqual(
            {
                (None, Apri_Info(num = Apri_Info(descr = "hi"), two = Apri_Info(descr = "hi"))),
                ("num", Apri_Info(descr = "hi")),
                ("two", Apri_Info(descr = "hi"))
            },
            set(apri.iter_inner_info())
        )

        self.assertEqual(
            3,
            sum(1 for _ in apri.iter_inner_info())
        )

        apri = Apri_Info(num = Apri_Info(descr = "hey"), two = Apri_Info(descr = "hi"))

        self.assertEqual(
            {
                (None, Apri_Info(num = Apri_Info(descr = "hey"), two = Apri_Info(descr = "hi"))),
                ("num", Apri_Info(descr = "hey")),
                ("two", Apri_Info(descr = "hi"))
            },
            set(apri.iter_inner_info())
        )

        self.assertEqual(
            3,
            sum(1 for _ in apri.iter_inner_info())
        )

    def test_change_info(self):

        apri = Apri_Info(descr = "descr")

        with self.assertRaises(TypeError):
            apri.change_info(apri, 0)

        with self.assertRaises(TypeError):
            apri.change_info(0, apri)

        replaced = apri.change_info(Apri_Info(no = "yes"), Apri_Info(maybe = "maybe"))

        self.assertEqual(
            Apri_Info(descr = "descr"),
            replaced
        )

        replaced = apri.change_info(apri, Apri_Info(no = "yes"))

        self.assertEqual(
            Apri_Info(no = "yes"),
            replaced
        )

        apri = Apri_Info(descr = Apri_Info(num = 7))

        replaced = apri.change_info(Apri_Info(num = 7), Apri_Info(_num = 8))

        self.assertEqual(
            Apri_Info(descr = Apri_Info(_num = 8)),
            replaced
        )

        replaced = apri.change_info(apri, Apri_Info(hello = "hi"))

        self.assertEqual(
            Apri_Info(hello = "hi"),
            replaced
        )

        apri = Apri_Info(descr = Apri_Info(blub = Apri_Info(hi = "hello")))

        replaced = apri.change_info(Apri_Info(hi = "hello"), Apri_Info(hi = "hellox"))

        self.assertEqual(
            Apri_Info(descr = Apri_Info(blub = Apri_Info(hi = "hellox"))),
            replaced
        )

        replaced = apri.change_info(Apri_Info(blub = Apri_Info(hi = "hello")), Apri_Info(bloob = Apri_Info(hi = "hello")))

        self.assertEqual(
            Apri_Info(descr = Apri_Info(bloob = Apri_Info(hi = "hello"))),
            replaced
        )

        replaced = apri.change_info(Apri_Info(descr = Apri_Info(blub = Apri_Info(hi = "hello"))), Apri_Info(descr = "yes"))

        self.assertEqual(
            Apri_Info(descr = "yes"),
            replaced
        )

        apri = Apri_Info(num = 7, descr = Apri_Info(blub = Apri_Info(hi = "hello")))

        replaced = apri.change_info(Apri_Info(blub = Apri_Info(hi = "hello")), Apri_Info(bloob = Apri_Info(hi = "hello")))

        self.assertEqual(
            Apri_Info(num = 7, descr = Apri_Info(bloob = Apri_Info(hi = "hello"))),
            replaced
        )

        replaced = apri.change_info(Apri_Info(descr = Apri_Info(blub = Apri_Info(hi = "hello"))), Apri_Info(descr = "yes"))

        self.assertEqual(
            Apri_Info(num = 7, descr = Apri_Info(blub = Apri_Info(hi = "hello"))),
            replaced
        )

        replaced = apri.change_info(Apri_Info(num = 7, descr = Apri_Info(blub = Apri_Info(hi = "hello"))), Apri_Info(loot = "chest"))

        self.assertEqual(
            Apri_Info(loot = "chest"),
            replaced
        )

        apri = Apri_Info(num = Apri_Info(descr = "hi"), two = Apri_Info(descr = "hi"))

        replaced = apri.change_info(Apri_Info(descr = "hi") , Apri_Info(num = Apri_Info(descr = "hi")))

        self.assertEqual(
            Apri_Info(num = Apri_Info(num = Apri_Info(descr = "hi")), two = Apri_Info(num = Apri_Info(descr = "hi"))),
            replaced
        )

        apri = Apri_Info(num = Apri_Info(descr = "hey"), two = Apri_Info(descr = "hi"))

        replaced = apri.change_info(Apri_Info(descr = "hi") , Apri_Info(num = Apri_Info(descr = "hi")))

        self.assertEqual(
            Apri_Info(num = Apri_Info(descr = "hey"), two = Apri_Info(num = Apri_Info(descr = "hi"))),
            replaced
        )