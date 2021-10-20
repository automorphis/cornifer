import shutil
import time
from pathlib import Path
from unittest import TestCase

from cornifer.utilities import intervals_overlap, random_unique_filename, check_has_method, \
    replace_lists_with_tuples

"""
- LEVEL 0:
    - intervals_overlap
        - input check: negative length
        - edge cases: boundaries intersect, length of interval is 0
    - random_unique_filename
        - no input check
        - check suffix matches
        - don't worry about length, alphabet, num_attempts optional args
    - check_has_method
        - no input check
        - edge cases:
    - replace_lists_with_tuples
        - no input check
        - check tuples stay tuples
        - edge cases: empty list, empty dict, empty tuple
    - replace_tuples_with_lists
        - no input check
        - check lists stay lists
        - edge cases: empty list, empty dict, empty tuple
    - _justify_slice_start_stop
        - no input check
        - edge cases: everything is an edge case lol

- LEVEL 1:
    - justify_slice (_justify_slice_start_stop)
        - no input check
        - edge cases: everything is an edge case lol
"""

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
            self.assertEqual(intervals_overlap(int1, int2), ret, f"{int1}, {int2}, {ret}")
            self.assertEqual(intervals_overlap(int2, int1), ret, f"{int1}, {int2}, {ret}")

        for (int1, int2), error in bad_cases.items():
            with self.assertRaises(error):
                intervals_overlap(int1, int2)
            with self.assertRaises(error):
                intervals_overlap(int2, int1)

    def test_random_unique_filename(self):

        directory = Path(f"D:/tmp/test_random_unique_filename{int(time.time()*100)}")

        if directory.is_file():
            directory.unlink()
        if directory.is_dir():
            shutil.rmtree(directory)

        Path(directory).mkdir()

        exts = ["", ".txt", ".csv", ".pkl", ".npy"]

        files = set()
        for ext in exts:
            num_subdirs = 1000
            for _ in range(num_subdirs):
                file = random_unique_filename(directory, ext)
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

        self.assertTrue(check_has_method(test, "yes"))
        self.assertFalse(check_has_method(test, "also_no"))
        self.assertFalse(check_has_method(test, "no"))

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
            self.assertEqual(replace_lists_with_tuples(inp), out)

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

    def test__justify_slice_start_stop(self):

