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
from abc import ABC, abstractmethod

from cornifer.errors import Keyword_Argument_Error
from cornifer.utilities import replace_lists_with_tuples, replace_tuples_with_lists, order_json_obj


class _Info(ABC):

    def __init__(self, **kwargs):
        self.__dict__.update(kwargs)
        self._reserved_kws = ["_reserved_kws"]

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
        kwargs = replace_tuples_with_lists(self.__dict__)
        for kw in self._reserved_kws:
            kwargs.pop(kw,None)
        kwargs = order_json_obj(kwargs)
        try:
            ret = json.dumps(kwargs,
                ensure_ascii = True,
                allow_nan = True,
                indent = None,
                separators = (',', ':')
            )
        except (TypeError, ValueError):
            raise Keyword_Argument_Error(
                "One of the keyword arguments used to construct this instance cannot be encoded into " +
                "JSON. Use different keyword arguments, or override the " +
                f"classmethod `{self.__class__.__name__}.from_json` and the instancemethod " +
                f"`{self.__class__.__name__}.to_json`." # TODO change
            )
        if "\0" in ret:
            raise Keyword_Argument_Error(
                "One of the keyword arguments used to construct this instance contains the null character " +
                "'\\0'. Use different keyword arguments, or blah blah" # TODO change
            )
        return ret

    def _add_reserved_kw(self, kw):
        self._reserved_kws.append(kw)

    def _check_reserved_kws(self, kwargs):
        if any(kw in kwargs for kw in self._reserved_kws):
            raise Keyword_Argument_Error(
                "The following keyword-argument keys are reserved. Choose a different key.\n" +
                f"{', '.join(self._reserved_kws)}"
            )

    @abstractmethod
    def __hash__(self):pass

    def __eq__(self, other):
        return type(self) == type(other) and self.to_json() == other.to_json()

    def __str__(self):
        ret = f"{self.__class__.__name__}("
        first = True
        for key,val in self.__dict__.items():
            if key not in self._reserved_kws:
                if first:
                    first = False
                else:
                    ret += ", "
                ret += f"{key}=" + repr(val)
        return ret + ")"

    def __repr__(self):
        return str(self)

    def __copy__(self):
        info = type(self)()
        info.__dict__.update(self.__dict__)
        return info

    def __deepcopy__(self, memo):
        return self.__copy__()


class Apri_Info(_Info):

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

        self._add_reserved_kw("_json")
        self._add_reserved_kw("_hash")
        self._check_reserved_kws(kwargs)

        self._json = super().to_json()
        self._hash = hash(type(self))
        for key,val in kwargs.items():
            try:
                self._hash += hash(val)
            except (TypeError, AttributeError):
                raise Keyword_Argument_Error(
                    f"All keyword arguments must be hashable types. The keyword argument given by \"{key}\" "+
                    f"not a hashable type. The type of that argument is `{val.__class__.__name__}`."
                )

    def to_json(self):
        return self._json

    def __hash__(self):
        return self._hash


class Apos_Info(_Info):

    def __hash__(self):
        raise TypeError("`Apos_Info` is not a hashable type.")