"""
    Cornifer, an intuitive data manager for empirical and computational mathematics.
    Copyright (C) 2021 Michael P. Lane

    This program is free software: you can redistribute it and/or modify
    it under the terms of the GNU General Public License as published by
    the Free Software Foundation, either version 3 of the License, or
    (at your option) any later version.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU General Public License for more details.
"""
import sys
import warnings

import numpy as np

from cornifer import ApriInfo
from cornifer._utilities import checkHasMethod, justifySlice, isInt


class Block:

    def __init__(self, segment, apri, startn = 0):

        if not isinstance(apri, ApriInfo):
            raise TypeError("`apri` must be of type `Apri_Info`.")

        if not isInt(startn):
            raise TypeError("`startn` must be of type `int`.")

        else:
            startn = int(startn)

        if startn < 0:
            raise ValueError("`startn` must be non-negative.")

        self._custom_dtype = False

        if isinstance(segment, list):
            self._dtype = "list"

        elif isinstance(segment, np.ndarray):
            self._dtype = "ndarray"

        elif not checkHasMethod(segment, "__len__"):
            raise ValueError(
                f"`len(segment)` must be defined. Please define the method `__len__` for the type " +
                f"`{segment.__class__.__name__}`."
            )

        else:

            self._dtype = str(type(segment))
            self._custom_dtype = True

        self._startn = startn
        self._apri = apri
        self._seg = segment
        self._seg_ndarray = None

    def _check_and_warn_custom_get_ndarray(self, methodName):

        if self._custom_dtype and not self._seg_ndarray and not checkHasMethod(self._seg, methodName):

            try:
                self._seg_ndarray = self._seg.get_ndarray()

            except NameError:
                raise NotImplementedError(
                    f"If you have not implemented `{methodName}` for the type" +
                    f" `{self._seg.__class__.__name__}`, then you must implement the method " +
                    f"`get_ndarray()` for the type `{self._seg.__class__.__name__}`."
                )

            warnings.warn(
                f"The custom type `{self._seg.__class__.__name__}` has not defined the method" +
                f" `{methodName}`. Cornifer is calling the method `get_ndarray`, which may slow down the " +
                f"program or lead to unexpected behavior."
            )

            return False

        else:
            return True

    def segment(self):
        return self._seg

    def apri(self):
        return self._apri

    def startn(self):
        return self._startn

    def setStartn(self, startn):

        if not isInt(startn):
            raise TypeError("`startn` must be of type `int`")
        else:
            startn = int(startn)

        if startn < 0:
            raise ValueError("`startn` must be positive")

        self._startn = startn

    def subdivide(self, subintervalLen):

        if not isInt(subintervalLen):
            raise TypeError("`subintervalLen` must be an integer")
        else:
            subintervalLen = int(subintervalLen)

        if subintervalLen <= 1:
            raise ValueError("`subintervalLen` must be at least 2")

        startn = self.startn()
        return [
            self[i : i + subintervalLen]
            for i in range(startn, startn + len(self), subintervalLen)
        ]

    def __getitem__(self, item):

        if isinstance(item, tuple):
            raise IndexError(
                "`blk[]` cannot take more than one index."
            )

        elif isinstance(item, slice):

            apri = self.apri()
            startn = self.startn()
            length = len(self)
            item = justifySlice(item, startn, startn + length - 1)

            if not self._check_and_warn_custom_get_ndarray("__getitem__"):
                return Block(self._seg_ndarray[item, ...], apri, startn)

            elif self._dtype == "ndarray":
                return Block(self._seg[item, ...], apri, startn)

            else:
                return Block(self._seg[item], apri, startn)

        else:

            if item not in self:
                raise IndexError(
                    f"Indices must be between {self.startn()} and {self.startn() + len(self) - 1}" +
                    ", inclusive."
                )

            item -= self.startn()

            if not self._check_and_warn_custom_get_ndarray("__getitem__"):
                return self._seg_ndarray[item]

            else:
                return self._seg[item]

    def __len__(self):

        if self._dtype == "ndarray":
            return self._seg.shape[0]

        else:
            return len(self._seg)

    def __contains__(self, n):
        startn = self.startn()
        return startn <= n < startn + len(self)

    def __hash__(self):
        raise TypeError(
            f"The type `{self.__class__.__name__}` is not hashable. Please instead hash " +
            f"`(blk.apri(), blk.startn(), len(blk))`."
        )

    def __str__(self):
        ret = self.__class__.__name__ + "("
        ret += f"<{self._dtype}>:{len(self)}, "
        ret += repr(self._apri) + ", "
        ret += str(self._startn) + ")"
        return ret

    def __repr__(self):
        return str(self)

    def __eq__(self, other):

        if (
            type(self) != type(other) or self._dtype != other._dtype or
            self.apri() != other.apri() or self.startn() != other.startn() or
            len(self) != len(other)
        ):
            return False

        if not self._check_and_warn_custom_get_ndarray("__eq__"):
            other._check_and_warn_custom_get_ndarray("__eq__")
            return np.all(self._seg_ndarray == other._seg_ndarray)

        elif self._dtype == "ndarray":
            return np.all(self._seg == other._seg)

        else:
            return self._seg == other._seg

class MemmapBlock (Block):

    def __init__(self, segment, apri, startn = 0):

        if not isinstance(segment, np.memmap):
            raise TypeError("`segment` must be of type `np.memmap`.")

        super().__init__(segment, apri, startn)

    def close(self):
        """Close NumPy `memmap` handle.

        This method won't always work because NumPy doesn't provide an API for closing memmap handles. This works by
        deleting the `self._seg` reference and hoping that the garbage collector will close it. This method definitely
        will not work if there are references to `self._seg` outside of this instance.
        """

        try:

            if sys.getrefcount(self._seg) != 2:
                raise RuntimeError("Couldn't close the `memmap` handle.")

            del self._seg

        except AttributeError:
            pass



