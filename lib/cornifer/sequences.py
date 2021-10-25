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

import json
import warnings

import numpy as np

from cornifer.errors import Sequence_Description_Keyword_Argument_Error
from cornifer.utilities import check_has_method, replace_lists_with_tuples, replace_tuples_with_lists, \
    justify_slice, order_json_obj


class Sequence_Description:

    def __init__(self, **kwargs):

        if "_json" in kwargs.keys() or "_hash" in kwargs.keys():
            raise Sequence_Description_Keyword_Argument_Error(
                "The keyword-argument keys \"_json\" and \"_hash\" are reserved. Please choose a different " +
                "key."
            )

        self._hash = hash(type(self))
        for key,val in kwargs.items():
            try:
                self._hash += hash(val)
            except (TypeError, AttributeError):
                raise Sequence_Description_Keyword_Argument_Error(
                    f"All keyword arguments must be hashable types. The keyword argument given by \"{key}\" "+
                    f"not a hashable type. The type of that argument is `{val.__class__.__name__}`."
                )

        self.__dict__.update(kwargs)

        self._json = None
        self._json = self.to_json()

        # try:
        self._json.encode("ASCII")
        # except UnicodeEncodeError:
        #     raise Sequence_Description_Keyword_Argument_Error(
        #         "`descr.to_json()` returns invalid JSON because it contains a non-ASCII character.\nPlease " +
        #         "use different keyword-arguments that do not contain non-ASCII characters when you construct"+
        #         "`Sequence_Description`, or override `to_json` so that it does not return a string that " +
        #         "contains non-ASCII characters." +
        #         f"`descr.to_json()` returns:\n{self._json}"
        #     )

        if "\0\0" in self._json:
            raise Sequence_Description_Keyword_Argument_Error(
                "`descr._tojson()` returns invalid JSON because it contains the double-null "+
                "substring \"\\0\\0\". This substring is reserved because it is used as a separator.\n"+
                "Please use different keyword-arguments that do not contain \"\\0\\0\" when you construct "+
                "`Sequence_Description`, or override `to_json` so that it does not return a string that "+
                "contains \"\\0\\0\".\n" +
                f"`descr.to_json()` returns:\n{self._json}"
            )

    @classmethod
    def from_json(cls, json_string):
        json_obj = json.loads(json_string)
        if not isinstance(json_obj, dict):
            raise TypeError(
                "The outermost layer of the passed `json_string` must be a JavaScript `object`, that is, " +
                f"a Python `dict`. The outermost layer of the passed `json_string` is: " +
                f"`{json_obj.__class__.__name__}`."
            )
        return cls(**replace_lists_with_tuples(json_obj))

    def to_json(self):
        if self._json is None:
            kwargs = replace_tuples_with_lists(self.__dict__)
            del kwargs["_json"]
            del kwargs["_hash"]
            kwargs = order_json_obj(kwargs)
            try:
                return json.dumps(kwargs,
                    ensure_ascii = True,
                    allow_nan = True,
                    indent = None,
                    separators = (',', ':')
                )
            except (TypeError, ValueError):
                raise Sequence_Description_Keyword_Argument_Error(
                    "One of the keyword arguments used to construct this instance cannot be encoded into " +
                    "JSON. Please reconstruct this instance using different arguments, or override the " +
                    f"classmethod `{self.__class__.__name__}.from_json` and the instancemethod " +
                    f"`{self.__class__.__name__}.to_json`."
                )
        else:
            return self._json

    def __hash__(self):
        return self._hash

    def __eq__(self, other):
        return type(self) == type(other) and self.to_json() == other.to_json()

    def __copy__(self):
        descr = Sequence_Description()
        descr.__dict__.update(self.__dict__)
        return descr

    def __deepcopy__(self, memo):
        return self.__copy__()

class Block:

    def __init__(self, data, descr, start_n = 0):

        self._custom_dtype = False

        if not isinstance(descr, Sequence_Description):
            raise TypeError(
                f"`descr` must be a `Sequence_Description` derived type. Passed " +
                f"type: {descr.__class__.__name__}"
            )

        elif not isinstance(start_n, int):
            raise TypeError("`start_n` must be an integer")

        elif start_n < 0:
            raise ValueError("`start_n` must be non-negative")


        if isinstance(data, list):
            self._dtype = "list"

        elif isinstance(data, np.ndarray):
            self._dtype = "ndarray"

        elif not check_has_method(data, "__len__"):
            raise ValueError(
                f"`len(data)` must be defined. Please define the method `__len__` for the type " +
                f"`{data.__class__.__name__}`."
            )

        else:
            self._dtype = str(type(data))
            self._custom_dtype = True

        self._start_n = start_n
        self._descr = descr
        self._data = data
        self._data_ndarray = None

    def _check_and_warn_custom_get_ndarray(self, method_name):

        if self._custom_dtype and not self._data_ndarray and not check_has_method(self._data, method_name):
            try:
                self._data_ndarray = self._data.get_ndarray()
            except NameError:
                raise NotImplementedError(
                    f"If you have not implemented `{method_name}` for the type" +
                    f" `{self._data.__class__.__name__}`, then you must implement the method " +
                    f"`get_ndarray()` for the type `{self._data.__class__.__name__}`."
                )
            warnings.warn(
                f"The custom type `{self._data.__class__.__name__}` has not defined the method" +
                f" `{method_name}`. The API is calling the method `get_ndarray`, which may slow down the " +
                f"program or lead to unexpected behavior."
            )
            return False

        else:
            return True

    def get_data(self):
        return self._data

    def get_descr(self):
        return self._descr

    def get_start_n(self):
        return self._start_n

    def set_start_n(self, start_n):
        if not isinstance(start_n, int):
            raise TypeError("`start_n` must be an integer")
        elif start_n < 0:
            raise ValueError("`start_n` must be positive")
        self._start_n = start_n

    def subdivide(self, subinterval_length):
        if not isinstance(subinterval_length, int):
            raise TypeError("`subinterval_length` must be an integer")
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
            descr = self.get_descr()
            start_n = self.get_start_n()
            length = len(self)
            item = justify_slice(item, start_n, start_n + length - 1)

            if not self._check_and_warn_custom_get_ndarray("__getitem__"):
                return Block(self._data_ndarray[item, ...], descr, start_n)
            elif self._dtype == "ndarray":
                return Block(self._data[item, ...], descr, start_n)
            else:
                return Block(self._data[item], descr, start_n)

        else:
            if item not in self:
                raise IndexError(
                    f"Indices must be between {self.get_start_n()} and {self.get_start_n() + len(self) - 1}" +
                    ", inclusive."
                )

            if not self._check_and_warn_custom_get_ndarray("__getitem__"):
                return self._data_ndarray[item]
            else:
                return self._data[item]

    def __len__(self):
        if self._dtype == "ndarray":
            return self._data.shape[0]
        else:
            return len(self._data)

    def __contains__(self, n):
        start_n = self.get_start_n()
        return start_n <= n < start_n + len(self)

    def __hash__(self):
        raise TypeError(
            f"The type `{self.__class__.__name__}` is not hashable. Please instead hash " +
            f"`(blk.get_descr(), blk.get_start_n(), len(blk))`."
        )

    def __eq__(self, other):

        if (
            type(self) != type(other) or self._dtype != other._dtype or
            self.get_descr() != other.get_descr() or self.get_start_n() != other.get_start_n() or
            len(self) != len(other)
        ):
            return False

        if not self._check_and_warn_custom_get_ndarray("__eq__"):
            other._check_and_warn_custom_get_ndarray("__eq__")
            return np.all(self._data_ndarray == other._data_ndarray)

        elif self._dtype == "ndarray":
            return np.all(self._data == other._data)

        else:
            return self._data == other._data