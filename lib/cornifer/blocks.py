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

from cornifer import Apri_Info
from cornifer.utilities import check_has_method, justify_slice, is_signed_int


class Block:

    def __init__(self, segment, apri, start_n = 0):

        if not isinstance(apri, Apri_Info):
            raise TypeError("`apri` must be of type `Apri_Info`.")

        if not is_signed_int(start_n):
            raise TypeError("`start_n` must be of type `int`.")

        else:
            start_n = int(start_n)

        if start_n < 0:
            raise ValueError("`start_n` must be non-negative.")

        self._custom_dtype = False

        if isinstance(segment, list):
            self._dtype = "list"

        elif isinstance(segment, np.ndarray):
            self._dtype = "ndarray"

        elif not check_has_method(segment, "__len__"):
            raise ValueError(
                f"`len(segment)` must be defined. Please define the method `__len__` for the type " +
                f"`{segment.__class__.__name__}`."
            )

        else:

            self._dtype = str(type(segment))
            self._custom_dtype = True

        self._start_n = start_n
        self._apri = apri
        self._seg = segment
        self._seg_ndarray = None

    def _check_and_warn_custom_get_ndarray(self, method_name):

        if self._custom_dtype and not self._seg_ndarray and not check_has_method(self._seg, method_name):

            try:
                self._seg_ndarray = self._seg.get_ndarray()

            except NameError:
                raise NotImplementedError(
                    f"If you have not implemented `{method_name}` for the type" +
                    f" `{self._seg.__class__.__name__}`, then you must implement the method " +
                    f"`get_ndarray()` for the type `{self._seg.__class__.__name__}`."
                )

            warnings.warn(
                f"The custom type `{self._seg.__class__.__name__}` has not defined the method" +
                f" `{method_name}`. Cornifer is calling the method `get_ndarray`, which may slow down the " +
                f"program or lead to unexpected behavior."
            )

            return False

        else:
            return True

    def get_segment(self):
        return self._seg

    def get_apri(self):
        return self._apri

    def get_start_n(self):
        return self._start_n

    def set_start_n(self, start_n):

        if not is_signed_int(start_n):
            raise TypeError("`start_n` must be of type `int`")
        else:
            start_n = int(start_n)

        if start_n < 0:
            raise ValueError("`start_n` must be positive")

        self._start_n = start_n

    def subdivide(self, subinterval_length):

        if not is_signed_int(subinterval_length):
            raise TypeError("`subinterval_length` must be an integer")
        else:
            subinterval_length = int(subinterval_length)

        if subinterval_length <= 1:
            raise ValueError("`subinterval_length` must be at least 2")

        start_n = self.get_start_n()
        return [
            self[i : i + subinterval_length]
            for i in range(start_n, start_n + len(self), subinterval_length)
        ]

    def __getitem__(self, item):

        if isinstance(item, tuple):
            raise IndexError(
                "`blk[]` cannot take more than one dimension of indices."
            )

        elif isinstance(item, slice):

            apri = self.get_apri()
            start_n = self.get_start_n()
            length = len(self)
            item = justify_slice(item, start_n, start_n + length - 1)

            if not self._check_and_warn_custom_get_ndarray("__getitem__"):
                return Block(self._seg_ndarray[item, ...], apri, start_n)

            elif self._dtype == "ndarray":
                return Block(self._seg[item, ...], apri, start_n)

            else:
                return Block(self._seg[item], apri, start_n)

        else:

            if item not in self:
                raise IndexError(
                    f"Indices must be between {self.get_start_n()} and {self.get_start_n() + len(self) - 1}" +
                    ", inclusive."
                )

            item -= self.get_start_n()

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
        start_n = self.get_start_n()
        return start_n <= n < start_n + len(self)

    def __hash__(self):
        raise TypeError(
            f"The type `{self.__class__.__name__}` is not hashable. Please instead hash " +
            f"`(blk.get_apri(), blk.get_start_n(), len(blk))`."
        )

    def __str__(self):
        ret = self.__class__.__name__ + "("
        ret += f"<{self._dtype}>:{len(self)}, "
        ret += repr(self._apri) + ", "
        ret += str(self._start_n) + ")"
        return ret

    def __repr__(self):
        return str(self)

    def __eq__(self, other):

        if (
            type(self) != type(other) or self._dtype != other._dtype or
            self.get_apri() != other.get_apri() or self.get_start_n() != other.get_start_n() or
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

class Memmap_Block (Block):

    def __init__(self, segment, apri, start_n = 0):

        if not isinstance(segment, np.memmap):
            raise TypeError("`segment` must be of type `np.memmap`.")

        super().__init__(segment, apri, start_n)

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



