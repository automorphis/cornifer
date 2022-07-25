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
from copy import copy, deepcopy

from cornifer._utilities import order_json_obj

class _Info_JSONEncoder(json.JSONEncoder):

    def default(self, obj):

        if isinstance(obj, _Info):
            return obj.__class__.__name__ + ".from_json(" + obj.to_json() + ")"

        elif isinstance(obj, tuple):
            return list(obj)

        else:
            return super().default(obj)

class _Info_JSONDecoder(json.JSONDecoder):

    def __init__(self, *args, **kwargs):
        super().__init__(object_hook=self.object_hook, *args, **kwargs)

    def object_hook(self, obj):

        if isinstance(obj, str):

            obj = obj.strip(" \t")
            if (obj[:9] == "Apri_Info" or obj[:9] == "Apos_Info") and obj[9:20] == ".from_json(" and obj[-1] == ")":

                json_str = obj[20:-1].strip(" \t")

                try:

                    if obj[:9] == "Apri_Info":
                        return Apri_Info.from_json(json_str)

                    else:
                        return Apos_Info.from_json(json_str)

                except json.JSONDecodeError:
                    return obj

            else:
                return obj

        elif isinstance(obj, dict):
            return {key : self.object_hook(val) for key, val in obj.items()}

        elif isinstance(obj, list):
            return tuple([self.object_hook(item) for item in obj])

        else:
            return obj

class _Info(ABC):

    _reserved_kws = ["_json"]

    def __init__(self, **kwargs):

        if len(kwargs) == 0:
            raise ValueError("must pass at least one keyword argument.")

        type(self)._check_reserved_kws(kwargs)

        self.__dict__.update(kwargs)

        self._json = None

    @classmethod
    def _check_reserved_kws(cls, kwargs):

        if any(kw in kwargs for kw in cls._reserved_kws):

            raise ValueError(

                "The following keyword-argument keys are reserved. Choose a different key.\n" +
                f"{', '.join(cls._reserved_kws)}"
            )

    def get_wrapped_string(self):

        return _Info_JSONEncoder().encode(self)

    @classmethod
    def from_json(cls, json_string):

        decoded_json = _Info_JSONDecoder().decode(json_string)

        if not isinstance(decoded_json, dict):
            raise ValueError(
                "The outermost layer of the passed `json_string` must be a JavaScript `object`, that is, " +
                f"a Python `dict`. The outermost layer of the passed `json_string` is: " +
                f"`{decoded_json.__class__.__name__}`."
            )

        return cls(**decoded_json)

    def to_json(self):

        if self._json is not None:
            return self._json

        else:

            kwargs = copy(self.__dict__)

            for kw in self._reserved_kws:
                kwargs.pop(kw,None)

            kwargs = order_json_obj(kwargs)

            try:
                json_rep = _Info_JSONEncoder(

                    ensure_ascii = True,
                    allow_nan = True,
                    indent = None,
                    separators = (',', ':')

                ).encode(kwargs)

            except (TypeError, ValueError):

                raise ValueError(
                    "One of the keyword arguments used to construct this instance cannot be encoded into " +
                    "JSON. Use different keyword arguments, or override the " +
                    f"classmethod `{self.__class__.__name__}.from_json` and the instancemethod " +
                    f"`{self.__class__.__name__}.to_json`."
                )

            if "\0" in json_rep:

                raise ValueError(
                    "One of the keyword arguments used to construct this instance contains the null character " +
                    "'\\0'."
                )

            self._json = json_rep

            return json_rep

    def iter_inner_info(self, _root_call = True):

        if not isinstance(_root_call, bool):
            raise TypeError("`_root_call` must be of type `bool`.")

        if _root_call:
            yield None, self

        for key, val in self.__dict__.items():

            if key not in type(self)._reserved_kws and isinstance(val, _Info):

                yield key, val

                for inner in val.iter_inner_info(_root_call = False):
                    yield inner

    def change_info(self, old_info, new_info, _root_call = True):

        if not isinstance(old_info, _Info):
            raise TypeError("`old_info` must be of type `_Info`.")

        if not isinstance(new_info, _Info):
            raise TypeError("`new_info` must be of type `_Info`.")

        if not isinstance(_root_call, bool):
            raise TypeError("`_root_call` must be of type `bool`.")

        if _root_call:
            replaced_info = deepcopy(self)

        else:
            replaced_info = self

        if self == old_info:
            return new_info

        else:

            kw = {}

            for key, val in replaced_info.__dict__.items():

                if key not in type(self)._reserved_kws:

                    if val == old_info:
                        kw[key] = new_info

                    elif isinstance(val, _Info):
                        kw[key] = val.change_info(old_info, new_info)

                    else:
                        kw[key] = val

            return type(self)(**kw)

    def __contains__(self, apri):

        if not isinstance(apri, Apri_Info):
            raise TypeError("`apri` must be of type `Apri_Info`.")

        return any(inner == apri for _, inner in self.iter_inner_info())

    def __lt__(self, other):

        if type(self) != type(other):
            return False

        else:
            return str(self) < str(other)

    def __gt__(self, other):

        if type(self) != type(other):
            return False

        else:
            return not(self < other)

    @abstractmethod
    def __hash__(self):pass

    def __eq__(self, other):
        return type(self) == type(other) and self.to_json() == other.to_json()

    def __str__(self):

        ret = f"{self.__class__.__name__}("

        first = True

        for key, val in self.__dict__.items():

            if key not in self._reserved_kws:

                if first:
                    first = False

                else:
                    ret += ", "

                ret += f"{key}={repr(val)}"

        return ret + ")"

    def __repr__(self):
        return str(self)

    def __copy__(self):
        info = type(self)(placeholder = "placeholder")
        del info.placeholder
        info.__dict__.update(self.__dict__)
        return info

    def __deepcopy__(self, memo):
        return self.__copy__()

class Apri_Info(_Info):

    _reserved_kws = ["_json", "_hash"]

    def __init__(self, **kwargs):

        super().__init__(**kwargs)

        self._hash = hash(type(self))

        for key,val in kwargs.items():

            try:
                self._hash += hash(val)

            except (TypeError, AttributeError):

                raise ValueError(
                    f"All keyword arguments must be hashable types. The keyword argument given by \"{key}\" "+
                    f"not a hashable type. The type of that argument is `{val.__class__.__name__}`."
                )

    def __hash__(self):
        return self._hash

class Apos_Info(_Info):

    def __hash__(self):
        raise TypeError("`Apos_Info` is not a hashable type.")

