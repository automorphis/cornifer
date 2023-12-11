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

import warnings
from abc import ABC, abstractmethod

import numpy as np

from .errors import BlockNotOpenError
from .info import ApriInfo
from ._utilities import check_has_method, justify_slice, is_int, check_type, check_return_int

class Block:

    def __init__(self, segment, apri, startn = 0):

        check_type(apri, "apri", ApriInfo)
        startn = check_return_int(startn, "startn")

        if startn < 0:
            raise ValueError("`startn` must be non-negative.")

        self.segment_type = type(segment)
        self._custom_type = not issubclass(self.segment_type, (list, np.ndarray))

        if not check_has_method(segment, "__len__"):
            raise ValueError(
                f"`len(segment)` must be defined. Please define the method `__len__` for the type " +
                f"`{segment.__class__.__name__}`."
            )

        self._startn = startn
        self._apri = apri
        self._seg = segment
        self._seg_ndarray = None
        self._num_entered = 0

    def _check_entered_raise(self, method_name):

        if self._num_entered == 0:
            raise BlockNotOpenError(f"You must do `with blk:` before you call `blk.{method_name}()`.")

    @classmethod
    def cast(cls, obj):

        check_type(obj, "obj", Block)
        return cls(obj._seg, obj._apri, obj._startn)

    def _check_and_warn_custom_get_ndarray(self, method_name):

        if self._custom_type and self._seg_ndarray is None and not check_has_method(self._seg, method_name):

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

    def segment(self):

        self._check_entered_raise("segment")
        return self._seg

    def apri(self):
        return self._apri

    def startn(self):
        return self._startn

    def set_startn(self, startn):

        startn = check_return_int(startn, "startn")

        if startn < 0:
            raise ValueError("`startn` must be positive")

        self._startn = startn

    def subdivide(self, subinterval_len):

        self._check_entered_raise("subdivide")
        subinterval_len = check_return_int(subinterval_len, "subinterval_len")

        if subinterval_len <= 1:
            raise ValueError("`subinterval_len` must be at least 2")

        startn = self.startn()
        return [
            self[i : i + subinterval_len]
            for i in range(startn, startn + len(self), subinterval_len)
        ]

    def __getitem__(self, item):

        self._check_entered_raise("__getitem__")

        if isinstance(item, tuple):
            raise IndexError("`blk[]` cannot take more than one index.")

        is_slice = isinstance(item, slice)

        if not is_int(item) and not is_slice:
            raise TypeError("`item` must be either of type `int` or `slice`.")

        elif not is_slice:
            item = int(item)

        if isinstance(item, slice):

            item = justify_slice(item, self.startn(), self.startn() + len(self) - 1)

            if not self._check_and_warn_custom_get_ndarray("__getitem__"):
                return self._seg_ndarray[item, ...]

            elif issubclass(self.segment_type, np.ndarray):
                return self._seg[item, ...]

            else:
                return self._seg[item]

        else:

            if item not in self:
                raise IndexError(
                    f"Indices must be between {self.startn()} and {self.startn() + len(self) - 1}, "
                    "inclusive."
                )

            item -= self.startn()

            if not self._check_and_warn_custom_get_ndarray("__getitem__"):
                return self._seg_ndarray[item]

            elif issubclass(self.segment_type, np.ndarray) and self._seg.ndim >= 2:
                return self._seg[item, ...]

            else:
                return self._seg[item]

    def __setitem__(self, key, value):

        self._check_entered_raise("__setitem__")

        if isinstance(key, slice):
            raise NotImplementedError("Support for slices coming soon.")

        key = check_return_int(key, "key")

        if isinstance(key, tuple):
            raise IndexError("`blk[]` cannot take more than one index.")

        if key not in self:
            raise IndexError(
                f"Indices must be between {self.startn()} and {self.startn() + len(self) - 1}" +
                ", inclusive."
            )

        key -= self.startn()

        if check_has_method(self._seg, "__setitem__"):

            if issubclass(self.segment_type, np.ndarray):
                self._seg[key, ...] = value

            else:
                self._seg[key] = value

        else:
            raise NotImplementedError

    def __len__(self):

        self._check_entered_raise("__len__")
        return len(self._seg)

    def __contains__(self, n):

        n = check_return_int(n, "n")

        startn = self.startn()
        return startn <= n < startn + len(self)

    def __hash__(self):
        raise TypeError(
            f"The type `{self.__class__.__name__}` is not hashable. Please instead hash " +
            f"`(blk.apri(), blk.startn_(), len(blk))`."
        )

    def __str__(self):
        
        ret = self.__class__.__name__ + "("
        ret += f"<{self.segment_type}>:{len(self)}, "
        ret += repr(self._apri) + ", "
        ret += str(self._startn) + ")"
        return ret

    def __repr__(self):
        return str(self)

    def __lt__(self, other):

        self._check_entered_raise("__lt__")
        return self._startn < other.startn or len(self) > len(other)

    def __gt__(self, other):

        self._check_entered_raise("__gt__")
        return self._startn > other.startn or len(self) < len(other)

    def __eq__(self, other):

        self._check_entered_raise("__eq__")

        if (
            type(self) != type(other) or self.segment_type != other.segment_type or
            self.apri() != other.apri() or self.startn() != other.startn() or
            len(self) != len(other)
        ):
            return False

        if not self._check_and_warn_custom_get_ndarray("__eq__"):
            other._check_and_warn_custom_get_ndarray("__eq__")
            return np.all(self._seg_ndarray == other._seg_ndarray)

        elif issubclass(self.segment_type, np.ndarray):
            return np.all(self._seg == other._seg)

        else:
            return self._seg == other._seg

    def __enter__(self):

        self._num_entered += 1
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._num_entered -= 1

class ReleaseBlock(Block, ABC):

    @abstractmethod
    def _release(self):
        """Release resources associated with this `Block`."""

    def __exit__(self, exc_type, exc_val, exc_tb):

        self._release()
        super().__exit__(exc_type, exc_val, exc_tb)


class MemmapBlock(ReleaseBlock):

    def __init__(self, segment, apri, startn = 0):

        if not isinstance(segment, np.memmap):
            raise TypeError("`segment` must be of type `np.memmap`.")

        self._filepath = segment.filename
        self._dtype = segment.dtype
        self._mode = segment.mode
        self._offset = segment.offset
        self._shape = segment.shape

        super().__init__(segment, apri, startn)

    def __enter__(self):

        if not hasattr(self, "_seg"):
            self._seg = np.memmap(self._filepath, self._dtype, self._mode, self._offset, self._shape)

        return super().__enter__()

    def _release(self):
        """Attempt to release NumPy memmap resources.

        This method simply deletes the `_seg` attribute of this `Block`. If no references to the data that `seg`
        point(ed) to remain anywhere in the runtime, then the garbage collector will free the memmap resources.
        NumPy provides no interface for manually freeing memmap resources, so this is the best we can do.

        The following possibilities are NOT mutually exclusive:
            - The user is following correct cornifer coding practice.
            - One or more references to the data that `_seg` point(ed) to remain after this method returns.

        Consider the following snippet:

            a = np.memmap(...)           # 1 reference  (a)
            blk1 = MemmapBlock(a, apri)  # 2 referneces (a, blk1._seg)
            with blk1:
                ...
            ...                          # 1 reference  (a)

        Indeed, the reference to the `a` memmap will remain until the user manually deletes the reference via
        `del a`.
        """

        try:
            del self._seg

        except AttributeError:
            pass

    def change_mode(self, mode):

        if not isinstance(mode, str):
            raise TypeError("`mode` must be a string.")

        filename = self._seg.filename
        self._seg.flush()
        self._release()
        self._seg = np.memmap(filename, mode = mode)


