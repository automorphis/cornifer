import shutil
import time
from pathlib import Path
from unittest import TestCase

from cornifer._utilities import intervalsOverlap, randomUniqueFilename, checkHasMethod, \
    replaceListsWithTuples, replaceTuplesWithLists, _justifySliceStartStop, orderJsonObj

"""
- LEVEL 0:
    - intervalsOverlap
        - input check: negative length_
        - edge cases: boundaries intersect, length_ of interval is 0
    - randomUniqueFilename
        - no input check
        - check suffix matches
        - don't worry about length_, alphabet, num_attempts optional args
    - checkHasMethod
        - no input check
        - edge cases:
    - replaceListsWithTuples
        - no input check
        - check tuples stay tuples
        - edge cases: empty list, empty dict, empty tuple
    - replaceTuplesWithLists
        - no input check
        - check lists stay lists
        - edge cases: empty list, empty dict, empty tuple
    - _justifySliceStartStop
        - no input check
        - edge cases: everything is an edge case lol

- LEVEL 1:
    - justifySlice (_justifySliceStartStop)
        - input check: `minIndex > maxIndex`
        - edge cases: everything is an edge case lol
"""

SAVES_DIR = Path(__file__).parent.resolve() / "temp"

test__justify_slice_start_tests = {
            (-6, 0, 0) : 0,
            (-5, 0, 0) : 0,
            (-4, 0, 0) : 0,
            (-3, 0, 0) : 0,
            (-2, 0, 0) : 0,
            (-1, 0, 0) : 0,
            ( 0, 0, 0) : 0,
            ( 1, 0, 0) : 1,
            ( 2, 0, 0) : 1,
            ( 3, 0, 0) : 1,
            ( 4, 0, 0) : 1,
            ( 5, 0, 0) : 1,
            ( 6, 0, 0) : 1,

            (-6, 1, 1) : 0,
            (-5, 1, 1) : 0,
            (-4, 1, 1) : 0,
            (-3, 1, 1) : 0,
            (-2, 1, 1) : 0,
            (-1, 1, 1) : 0,
            ( 0, 1, 1) : 0,
            ( 1, 1, 1) : 0,
            ( 2, 1, 1) : 1,
            ( 3, 1, 1) : 1,
            ( 4, 1, 1) : 1,
            ( 5, 1, 1) : 1,
            ( 6, 1, 1) : 1,

            (-6, 2, 2) : 0,
            (-5, 2, 2) : 0,
            (-4, 2, 2) : 0,
            (-3, 2, 2) : 0,
            (-2, 2, 2) : 0,
            (-1, 2, 2) : 0,
            ( 0, 2, 2) : 0,
            ( 1, 2, 2) : 0,
            ( 2, 2, 2) : 0,
            ( 3, 2, 2) : 1,
            ( 4, 2, 2) : 1,
            ( 5, 2, 2) : 1,
            ( 6, 2, 2) : 1,

            (-6, 0, 1) : 0,
            (-5, 0, 1) : 0,
            (-4, 0, 1) : 0,
            (-3, 0, 1) : 0,
            (-2, 0, 1) : 0,
            (-1, 0, 1) : 1,
            ( 0, 0, 1) : 0,
            ( 1, 0, 1) : 1,
            ( 2, 0, 1) : 2,
            ( 3, 0, 1) : 2,
            ( 4, 0, 1) : 2,
            ( 5, 0, 1) : 2,
            ( 6, 0, 1) : 2,

            (-6, 1, 2) : 0,
            (-5, 1, 2) : 0,
            (-4, 1, 2) : 0,
            (-3, 1, 2) : 0,
            (-2, 1, 2) : 0,
            (-1, 1, 2) : 1,
            ( 0, 1, 2) : 0,
            ( 1, 1, 2) : 0,
            ( 2, 1, 2) : 1,
            ( 3, 1, 2) : 2,
            ( 4, 1, 2) : 2,
            ( 5, 1, 2) : 2,
            ( 6, 1, 2) : 2,

            (-6, 2, 3) : 0,
            (-5, 2, 3) : 0,
            (-4, 2, 3) : 0,
            (-3, 2, 3) : 0,
            (-2, 2, 3) : 0,
            (-1, 2, 3) : 1,
            ( 0, 2, 3) : 0,
            ( 1, 2, 3) : 0,
            ( 2, 2, 3) : 0,
            ( 3, 2, 3) : 1,
            ( 4, 2, 3) : 2,
            ( 5, 2, 3) : 2,
            ( 6, 2, 3) : 2,

            (-6, 3, 4) : 0,
            (-5, 3, 4) : 0,
            (-4, 3, 4) : 0,
            (-3, 3, 4) : 0,
            (-2, 3, 4) : 0,
            (-1, 3, 4) : 1,
            ( 0, 3, 4) : 0,
            ( 1, 3, 4) : 0,
            ( 2, 3, 4) : 0,
            ( 3, 3, 4) : 0,
            ( 4, 3, 4) : 1,
            ( 5, 3, 4) : 2,
            ( 6, 3, 4) : 2,

            (-6, 0, 2) : 0,
            (-5, 0, 2) : 0,
            (-4, 0, 2) : 0,
            (-3, 0, 2) : 0,
            (-2, 0, 2) : 1,
            (-1, 0, 2) : 2,
            ( 0, 0, 2) : 0,
            ( 1, 0, 2) : 1,
            ( 2, 0, 2) : 2,
            ( 3, 0, 2) : 3,
            ( 4, 0, 2) : 3,
            ( 5, 0, 2) : 3,
            ( 6, 0, 2) : 3,

            (-6, 1, 3) : 0,
            (-5, 1, 3) : 0,
            (-4, 1, 3) : 0,
            (-3, 1, 3) : 0,
            (-2, 1, 3) : 1,
            (-1, 1, 3) : 2,
            ( 0, 1, 3) : 0,
            ( 1, 1, 3) : 0,
            ( 2, 1, 3) : 1,
            ( 3, 1, 3) : 2,
            ( 4, 1, 3) : 3,
            ( 5, 1, 3) : 3,
            ( 6, 1, 3) : 3,

            (-6, 2, 4) : 0,
            (-5, 2, 4) : 0,
            (-4, 2, 4) : 0,
            (-3, 2, 4) : 0,
            (-2, 2, 4) : 1,
            (-1, 2, 4) : 2,
            ( 0, 2, 4) : 0,
            ( 1, 2, 4) : 0,
            ( 2, 2, 4) : 0,
            ( 3, 2, 4) : 1,
            ( 4, 2, 4) : 2,
            ( 5, 2, 4) : 3,
            ( 6, 2, 4) : 3,

            (-6, 3, 5) : 0,
            (-5, 3, 5) : 0,
            (-4, 3, 5) : 0,
            (-3, 3, 5) : 0,
            (-2, 3, 5) : 1,
            (-1, 3, 5) : 2,
            ( 0, 3, 5) : 0,
            ( 1, 3, 5) : 0,
            ( 2, 3, 5) : 0,
            ( 3, 3, 5) : 0,
            ( 4, 3, 5) : 1,
            ( 5, 3, 5) : 2,
            ( 6, 3, 5) : 3,

            (-6, 0, 3) : 0,
            (-5, 0, 3) : 0,
            (-4, 0, 3) : 0,
            (-3, 0, 3) : 1,
            (-2, 0, 3) : 2,
            (-1, 0, 3) : 3,
            ( 0, 0, 3) : 0,
            ( 1, 0, 3) : 1,
            ( 2, 0, 3) : 2,
            ( 3, 0, 3) : 3,
            ( 4, 0, 3) : 4,
            ( 5, 0, 3) : 4,
            ( 6, 0, 3) : 4,

            (-6, 1, 4) : 0,
            (-5, 1, 4) : 0,
            (-4, 1, 4) : 0,
            (-3, 1, 4) : 1,
            (-2, 1, 4) : 2,
            (-1, 1, 4) : 3,
            ( 0, 1, 4) : 0,
            ( 1, 1, 4) : 0,
            ( 2, 1, 4) : 1,
            ( 3, 1, 4) : 2,
            ( 4, 1, 4) : 3,
            ( 5, 1, 4) : 4,
            ( 6, 1, 4) : 4,

            (-6, 2, 5) : 0,
            (-5, 2, 5) : 0,
            (-4, 2, 5) : 0,
            (-3, 2, 5) : 1,
            (-2, 2, 5) : 2,
            (-1, 2, 5) : 3,
            ( 0, 2, 5) : 0,
            ( 1, 2, 5) : 0,
            ( 2, 2, 5) : 0,
            ( 3, 2, 5) : 1,
            ( 4, 2, 5) : 2,
            ( 5, 2, 5) : 3,
            ( 6, 2, 5) : 4,
        }

class Test___init__(TestCase):

    def test_intervals_overlap(self):
        good_cases = {
            ((0, 1), (1, 1)): False,
            ((0, 1), (1, 2)): False,
            ((-1, 2), (1, 2)): False,
            ((-1, 1), (1, 2)): False,
            ((-2, 2), (1, 2)): False,
            ((-2, 2), (1, 3)): False,

            ((5, 1), (6, 1)): False,
            ((5, 1), (6, 2)): False,
            ((4, 2), (6, 2)): False,
            ((4, 1), (6, 2)): False,
            ((3, 2), (6, 2)): False,
            ((3, 2), (6, 3)): False,

            ((1, 0), (1, 0)): False,
            ((1, 0), (1, 1)): False,
            ((1, 0), (-1, 10)): False,

            ((1, 1), (1, 1)): True,
            ((1, 1), (1, 2)): True,
            ((1, 2), (2, 1)): True,
            ((1, 1), (-1, 3)): True,
            ((1, 1), (-1, 2.5)): True,
            ((1, 2), (-1, 4)): True,
            ((1, 1), (0, 2)): True,
            ((1, 1), (0, 1.5)): True
        }

        bad_cases = {
            ((1, -1), (1, 1)): ValueError,
            ((1, -1), (1, -1)): ValueError
        }

        for (int1, int2), ret in good_cases.items():
            self.assertEqual(intervalsOverlap(int1, int2), ret, f"{int1}, {int2}, {ret}")
            self.assertEqual(intervalsOverlap(int2, int1), ret, f"{int1}, {int2}, {ret}")

        for (int1, int2), error in bad_cases.items():
            with self.assertRaises(error):
                intervalsOverlap(int1, int2)
            with self.assertRaises(error):
                intervalsOverlap(int2, int1)

    def test_random_unique_filename(self):

        if SAVES_DIR.is_dir():
            shutil.rmtree(SAVES_DIR)

        SAVES_DIR.mkdir()
        directory = SAVES_DIR / str(int(time.time()*100))
        directory.mkdir()

        exts = ["", ".txt", ".csv", ".pkl", ".npy"]

        files = set()
        for ext in exts:
            num_subdirs = 1000
            for _ in range(num_subdirs):
                file = randomUniqueFilename(directory, ext)
                files.add(file)
                file.touch()
                self.assertEqual(file.suffix, ext)
            self.assertEqual(len(files), len(list(directory.iterdir())))

        shutil.rmtree(directory)

    def test_check_has_method(self):

        class Test:
            def __init__(self):
                self.no = None
            def yes(self):pass

        test = Test()
        test.also_no = lambda x : x+1

        self.assertTrue(checkHasMethod(test, "yes"))
        self.assertFalse(checkHasMethod(test, "also_no"))
        self.assertFalse(checkHasMethod(test, "no"))

    def test_replace_lists_with_tuples(self):

        tests = [
            ( (), () ),
            ( [], () ),
            ( (0,), (0,) ),
            ( [0],  (0,) ),
            ( ("hey",), ("hey",) ),
            ( ["hey"], ("hey",) ),
            ( ((),), ((),) ),
            ( [()],  ((),) ),
            ( ([],), ((),) ),
            ( [[]],  ((),) ),

            ( (((),),), (((),),) ),
            ( ([()],),  (((),),) ),
            ( (([],),), (((),),) ),
            ( ([[]],),  (((),),) ),

            ( [((),)], (((),),) ),
            ( [[()]],  (((),),) ),
            ( [([],)], (((),),) ),
            ( [[[]]],  (((),),) ),

            ( (((0,),),), (((0,),),) ),
            ( ([(0,)],),  (((0,),),) ),
            ( (([0],),),  (((0,),),) ),
            ( ([[0]],),   (((0,),),) ),

            ( [((0,),)], (((0,),),) ),
            ( [[(0,)]],  (((0,),),) ),
            ( [([0],)],  (((0,),),) ),
            ( [[[0]]],   (((0,),),) ),

            ( (((),0),),  (((),0),) ),
            ( ([(),0],),  (((),0),) ),
            ( (([],0),),  (((),0),) ),
            ( ([[],0],),  (((),0),) ),

            ( [((),0)],   (((),0),) ),
            ( [[(),0]],   (((),0),) ),
            ( [([],0)],   (((),0),) ),
            ( [[[],0]],   (((),0),) ),

            ( (((),),0),  (((),),0) ),
            ( ([()], 0),  (((),),0) ),
            ( (([],),0),  (((),),0) ),
            ( ([[]], 0),  (((),),0) ),

            ( [((),),0],   (((),),0) ),
            ( [[()], 0],   (((),),0) ),
            ( [([],),0],   (((),),0) ),
            ( [[[]], 0],   (((),),0) ),

            ( {"hey": [[]], "hello": ((),)}, {"hey": ((),), "hello": ((),)}),

            ( [{"hey": [0]}], ({"hey": (0,)},))
        ]

        for inp, out in tests:
            self.assertEqual(replaceListsWithTuples(inp), out)

    def test_replace_tuples_with_lists(self):

        tests = [
            ( (), [] ),
            ( [], [] ),
            ( (0,), [0] ),
            ( [0],  [0] ),
            ( ("hey",), ["hey"] ),
            ( ["hey"], ["hey"] ),
            ( ((),), [[]] ),
            ( [()],  [[]] ),
            ( ([],), [[]] ),
            ( [[]],  [[]] ),

            ( (((),),), [[[]]] ),
            ( ([()],),  [[[]]] ),
            ( (([],),), [[[]]] ),
            ( ([[]],),  [[[]]] ),

            ( [((),)], [[[]]] ),
            ( [[()]],  [[[]]] ),
            ( [([],)], [[[]]] ),
            ( [[[]]],  [[[]]] ),

            ( (((0,),),), [[[0]]] ),
            ( ([(0,)],),  [[[0]]] ),
            ( (([0],),),  [[[0]]] ),
            ( ([[0]],),   [[[0]]] ),

            ( [((0,),)], [[[0]]] ),
            ( [[(0,)]],  [[[0]]] ),
            ( [([0],)],  [[[0]]] ),
            ( [[[0]]],   [[[0]]] ),

            ( (((),0),),  [[[],0]] ),
            ( ([(),0],),  [[[],0]] ),
            ( (([],0),),  [[[],0]] ),
            ( ([[],0],),  [[[],0]] ),

            ( [((),0)],   [[[],0]] ),
            ( [[(),0]],   [[[],0]] ),
            ( [([],0)],   [[[],0]] ),
            ( [[[],0]],   [[[],0]] ),

            ( (((),),0),  [[[]],0] ),
            ( ([()], 0),  [[[]],0] ),
            ( (([],),0),  [[[]],0] ),
            ( ([[]], 0),  [[[]],0] ),

            ( [((),),0],   [[[]],0] ),
            ( [[()], 0],   [[[]],0] ),
            ( [([],),0],   [[[]],0] ),
            ( [[[]], 0],   [[[]],0] ),

            ( {"hey": [[]], "hello": ((),)}, {"hey": [[]], "hello": [[]]}),

            ( ({"hey": (0,)},), [{"hey": [0]}])
        ]

        for inp, out in tests:
            self.assertEqual(replaceTuplesWithLists(inp), out)

    def test__justify_slice_start_stop(self):

        for inp, out in test__justify_slice_start_tests.items():
            self.assertEqual(
                _justifySliceStartStop(*inp),
                out,
                f"{inp}, {out}"
            )

    def test_order_json_obj(self):
        self.assertEqual(
            list(orderJsonObj({"xyz": 3, "abc": 4}).keys()),
            ["abc", "xyz"]
        )
        self.assertEqual(
            list(orderJsonObj({"abc": 3, "xyz": 4}).keys()),
            ["abc", "xyz"]
        )
        self.assertEqual(
            list(orderJsonObj({"xyz": {"abc":1, "xyz":2}, "abc": 4}).items()),
            [("abc",4), ("xyz", {"abc": 1, "xyz":2})]
        )
        self.assertEqual(
            list(orderJsonObj({"xyz": {"xyz":2, "abc":1}, "abc": 4}).items()),
            [("abc",4), ("xyz", {"abc": 1, "xyz":2})]
        )
        self.assertEqual(
            orderJsonObj([{"xyz":1, "abc":2}]),
            [{"abc":2, "xyz":1}]
        )