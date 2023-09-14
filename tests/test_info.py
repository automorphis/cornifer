import json
from copy import copy
from unittest import TestCase

from cornifer.info import ApriInfo, AposInfo


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

    def test_from_json(self):

        with self.assertRaises(ValueError):
            ApriInfo.from_json("[\"no\"]")

        apri = ApriInfo.from_json("{\"msg\": \"primes\"}")
        self.assertEqual(apri.msg, "primes")

        apri = ApriInfo.from_json("{\"mod4\": 1}")
        self.assertEqual(apri.mod4, 1)

        apri = ApriInfo.from_json("{\"tup\": [1,2,3]}")
        self.assertEqual(apri.tup, (1,2,3))

        apri = ApriInfo.from_json("""{"msg" : "primes", "respective" : "ApriInfo{\\"haha\\" : \\"lol\\"}"}""")
        self.assertEqual(apri.msg, "primes")
        self.assertEqual(apri.respective, ApriInfo(haha ="lol"))

        apos = AposInfo.from_json('{"haha":"ApriInfo{\\"four\\":5}","msg":"primes","respective":"AposInfo{\\"haha\\":\\"ApriInfo{\\\\\\"four\\\\\\":5}\\",\\"num\\":3}"}')
        self.assertEqual(apos.msg, "primes")
        self.assertEqual(apos.respective, AposInfo(num = 3, haha = ApriInfo(four = 5)))
        self.assertEqual(apos.haha, ApriInfo(four = 5))

        apri = ApriInfo.from_json(ApriInfo(msg = "primes", respective = ApriInfo(haha = "lol")).to_json())
        self.assertEqual(apri.msg, "primes")
        self.assertEqual(apri.respective, ApriInfo(haha = "lol"))


    def test_to_json(self):

        _json = ApriInfo(msg ="primes", mod4 = 3).to_json()
        self.assertTrue(isinstance(_json, str))
        obj = json.loads(_json)
        self.assertTrue(isinstance(obj, dict))
        self.assertEqual(len(obj), 2)
        self.assertEqual(obj, {"msg": "primes", "mod4": 3})

        _json = ApriInfo(msg="primes", primes = (2, 3, 5)).to_json()
        self.assertTrue(isinstance(_json, str))
        obj = json.loads(_json)
        self.assertTrue(isinstance(obj, dict))
        self.assertEqual(len(obj), 2)
        self.assertEqual(obj, {"msg": "primes", "primes": [2,3,5]})

        apri = ApriInfo(msg ="primes", primes = (2, 3, 5), respective = ApriInfo(lol ="haha"))
        self.assertEqual(apri, ApriInfo.from_json(apri.to_json()))

        apos = AposInfo(msg ="primes", respective = AposInfo(num = 3, haha = ApriInfo(four = 5)), haha = ApriInfo(four = 5))
        self.assertEqual(
            apos,
            AposInfo.from_json(apos.to_json())
        )

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
            set(apri.iter_inner_info())
        )

        self.assertEqual(
            1,
            sum(1 for _ in apri.iter_inner_info())
        )

        apri = ApriInfo(descr = ApriInfo(num = 7))

        self.assertEqual(
            {
                (None, ApriInfo(descr = ApriInfo(num = 7))),
                ("descr", ApriInfo(num = 7))
            },
            set(apri.iter_inner_info())
        )

        self.assertEqual(
            2,
            sum(1 for _ in apri.iter_inner_info())
        )

        apri = ApriInfo(descr = ApriInfo(blub = ApriInfo(hi ="hello")))

        self.assertEqual(
            {
                (None, ApriInfo(descr = ApriInfo(blub = ApriInfo(hi ="hello")))),
                ("descr", ApriInfo(blub = ApriInfo(hi ="hello"))),
                ("blub", ApriInfo(hi ="hello"))
            },
            set(apri.iter_inner_info())
        )

        self.assertEqual(
            3,
            sum(1 for _ in apri.iter_inner_info())
        )

        apri = ApriInfo(num = 7, descr = ApriInfo(blub = ApriInfo(hi ="hello")))

        self.assertEqual(
            {
                (None, ApriInfo(num = 7, descr = ApriInfo(blub = ApriInfo(hi ="hello")))),
                ("descr", ApriInfo(blub = ApriInfo(hi ="hello"))),
                ("blub", ApriInfo(hi ="hello"))
            },
            set(apri.iter_inner_info())
        )

        self.assertEqual(
            3,
            sum(1 for _ in apri.iter_inner_info())
        )

        apri = ApriInfo(num = 7, descr = ApriInfo(no ="yes", blub = ApriInfo(hi ="hello")))

        self.assertEqual(
            {
                (None, ApriInfo(num = 7, descr = ApriInfo(no ="yes", blub = ApriInfo(hi ="hello")))),
                ("descr", ApriInfo(no ="yes", blub = ApriInfo(hi ="hello"))),
                ("blub", ApriInfo(hi ="hello"))
            },
            set(apri.iter_inner_info())
        )

        self.assertEqual(
            3,
            sum(1 for _ in apri.iter_inner_info())
        )

        apri = ApriInfo(num = ApriInfo(descr ="hi"), two = ApriInfo(descr ="hi"))

        self.assertEqual(
            {
                (None, ApriInfo(num = ApriInfo(descr ="hi"), two = ApriInfo(descr ="hi"))),
                ("num", ApriInfo(descr ="hi")),
                ("two", ApriInfo(descr ="hi"))
            },
            set(apri.iter_inner_info())
        )

        self.assertEqual(
            3,
            sum(1 for _ in apri.iter_inner_info())
        )

        apri = ApriInfo(num = ApriInfo(descr ="hey"), two = ApriInfo(descr ="hi"))

        self.assertEqual(
            {
                (None, ApriInfo(num = ApriInfo(descr ="hey"), two = ApriInfo(descr ="hi"))),
                ("num", ApriInfo(descr ="hey")),
                ("two", ApriInfo(descr ="hi"))
            },
            set(apri.iter_inner_info())
        )

        self.assertEqual(
            3,
            sum(1 for _ in apri.iter_inner_info())
        )

    def test_change_info(self):

        apri = ApriInfo(descr ="descr")

        with self.assertRaises(TypeError):
            apri.change_info(apri, 0)

        with self.assertRaises(TypeError):
            apri.change_info(0, apri)

        replaced = apri.change_info(ApriInfo(no ="yes"), ApriInfo(maybe ="maybe"))

        self.assertEqual(
            ApriInfo(descr ="descr"),
            replaced
        )

        replaced = apri.change_info(apri, ApriInfo(no ="yes"))

        self.assertEqual(
            ApriInfo(no ="yes"),
            replaced
        )

        apri = ApriInfo(descr = ApriInfo(num = 7))

        replaced = apri.change_info(ApriInfo(num = 7), ApriInfo(_num = 8))

        self.assertEqual(
            ApriInfo(descr = ApriInfo(_num = 8)),
            replaced
        )

        replaced = apri.change_info(apri, ApriInfo(hello ="hi"))

        self.assertEqual(
            ApriInfo(hello ="hi"),
            replaced
        )

        apri = ApriInfo(descr = ApriInfo(blub = ApriInfo(hi ="hello")))

        replaced = apri.change_info(ApriInfo(hi ="hello"), ApriInfo(hi ="hellox"))

        self.assertEqual(
            ApriInfo(descr = ApriInfo(blub = ApriInfo(hi ="hellox"))),
            replaced
        )

        replaced = apri.change_info(ApriInfo(blub = ApriInfo(hi ="hello")), ApriInfo(bloob = ApriInfo(hi ="hello")))

        self.assertEqual(
            ApriInfo(descr = ApriInfo(bloob = ApriInfo(hi ="hello"))),
            replaced
        )

        replaced = apri.change_info(ApriInfo(descr = ApriInfo(blub = ApriInfo(hi ="hello"))), ApriInfo(descr ="yes"))

        self.assertEqual(
            ApriInfo(descr ="yes"),
            replaced
        )

        apri = ApriInfo(num = 7, descr = ApriInfo(blub = ApriInfo(hi ="hello")))

        replaced = apri.change_info(ApriInfo(blub = ApriInfo(hi ="hello")), ApriInfo(bloob = ApriInfo(hi ="hello")))

        self.assertEqual(
            ApriInfo(num = 7, descr = ApriInfo(bloob = ApriInfo(hi ="hello"))),
            replaced
        )

        replaced = apri.change_info(ApriInfo(descr = ApriInfo(blub = ApriInfo(hi ="hello"))), ApriInfo(descr ="yes"))

        self.assertEqual(
            ApriInfo(num = 7, descr = ApriInfo(blub = ApriInfo(hi ="hello"))),
            replaced
        )

        replaced = apri.change_info(ApriInfo(num = 7, descr = ApriInfo(blub = ApriInfo(hi ="hello"))), ApriInfo(loot ="chest"))

        self.assertEqual(
            ApriInfo(loot ="chest"),
            replaced
        )

        apri = ApriInfo(num = ApriInfo(descr ="hi"), two = ApriInfo(descr ="hi"))

        replaced = apri.change_info(ApriInfo(descr ="hi"), ApriInfo(num = ApriInfo(descr ="hi")))

        self.assertEqual(
            ApriInfo(num = ApriInfo(num = ApriInfo(descr ="hi")), two = ApriInfo(num = ApriInfo(descr ="hi"))),
            replaced
        )

        apri = ApriInfo(num = ApriInfo(descr ="hey"), two = ApriInfo(descr ="hi"))

        replaced = apri.change_info(ApriInfo(descr ="hi"), ApriInfo(num = ApriInfo(descr ="hi")))

        self.assertEqual(
            ApriInfo(num = ApriInfo(descr ="hey"), two = ApriInfo(num = ApriInfo(descr ="hi"))),
            replaced
        )