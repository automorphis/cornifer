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

from cornifer._utilities import orderJsonObj, isInt

class _InfoJsonEncoder(json.JSONEncoder):

    def default(self, obj):

        if isinstance(obj, _Info):
            return obj.__class__.__name__ + ".fromJson(" + obj.toJson() + ")"

        elif isInt(obj):
            return int(obj)

        elif isinstance(obj, tuple):
            return list(obj)

        else:
            return super().default(obj)

class _InfoJsonDecoder(json.JSONDecoder):

    def __init__(self, *args, **kwargs):
        super().__init__(object_hook=self.object_hook, *args, **kwargs)

    def object_hook(self, obj):

        if isinstance(obj, str):

            obj = obj.strip(" \t")
            if (obj[:8] == "ApriInfo" or obj[:8] == "AposInfo") and obj[8:18] == ".fromJson(" and obj[-1] == ")":

                json_str = obj[18:-1].strip(" \t")

                try:

                    if obj[:8] == "ApriInfo":
                        return ApriInfo.fromJson(json_str)

                    else:
                        return AposInfo.fromJson(json_str)

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

    _reservedKws = ["_json", "_str"]

    def __init__(self, **kwargs):

        if len(kwargs) == 0:
            raise ValueError("must pass at least one keyword argument.")

        type(self)._checkReservedKws(kwargs)

        self.__dict__.update(kwargs)

        self._json = None

        self._str = None

    @classmethod
    def _checkReservedKws(cls, kwargs):

        if any(kw in kwargs for kw in cls._reservedKws):

            raise ValueError(

                "The following keyword-argument keys are reserved. Choose a different key.\n" +
                f"{', '.join(cls._reservedKws)}"
            )

    @classmethod
    def fromJson(cls, jsonStr):

        decoded_json = _InfoJsonDecoder().decode(jsonStr)

        if not isinstance(decoded_json, dict):
            raise ValueError(
                "The outermost layer of the passed `json_string` must be a JavaScript `object`, that is, " +
                f"a Python `dict`. The outermost layer of the passed `json_string` is: " +
                f"`{decoded_json.__class__.__name__}`."
            )

        return cls(**decoded_json)

    def toJson(self):

        if self._json is not None:
            return self._json

        else:

            kwargs = copy(self.__dict__)

            for kw in type(self)._reservedKws:
                kwargs.pop(kw,None)

            kwargs = orderJsonObj(kwargs)

            try:
                json_rep = _InfoJsonEncoder(

                    ensure_ascii = True,
                    allow_nan = True,
                    indent = None,
                    separators = (',', ':')

                ).encode(kwargs)

            except (TypeError, ValueError) as e:

                raise ValueError(
                    "One of the keyword arguments used to construct this instance cannot be encoded into " +
                    "JSON. Use different keyword arguments, or override the " +
                    f"classmethod `{self.__class__.__name__}.fromJson` and the instancemethod " +
                    f"`{self.__class__.__name__}.toJson`."
                ) from e

            if "\0" in json_rep:

                raise ValueError(
                    "One of the keyword arguments used to construct this instance contains the null character " +
                    "'\\0'."
                )

            self._json = json_rep

            return json_rep

    def iterInnerInfo(self, _rootCall = True):

        if not isinstance(_rootCall, bool):
            raise TypeError("`_rootCall` must be of type `bool`.")

        if _rootCall:
            yield None, self

        for key, val in self.__dict__.items():

            if key not in type(self)._reservedKws and isinstance(val, _Info):

                yield key, val

                for inner in val.iterInnerInfo(_rootCall= False):
                    yield inner

    def changeInfo(self, oldInfo, newInfo, _rootCall = True):

        if not isinstance(oldInfo, _Info):
            raise TypeError("`oldInfo` must be of type `_Info`.")

        if not isinstance(newInfo, _Info):
            raise TypeError("`newInfo` must be of type `_Info`.")

        if not isinstance(_rootCall, bool):
            raise TypeError("`_rootCall` must be of type `bool`.")

        if _rootCall:
            replaced_info = deepcopy(self)

        else:
            replaced_info = self

        if self == oldInfo:
            return newInfo

        else:

            kw = {}

            for key, val in replaced_info.__dict__.items():

                if key not in type(self)._reservedKws:

                    if val == oldInfo:
                        kw[key] = newInfo

                    elif isinstance(val, _Info):
                        kw[key] = val.changeInfo(oldInfo, newInfo)

                    else:
                        kw[key] = val

            return type(self)(**kw)

    def __contains__(self, apri):

        if not isinstance(apri, ApriInfo):
            raise TypeError("`apri` must be of type `ApriInfo`.")

        return any(inner == apri for _, inner in self.iterInnerInfo())

    def __lt__(self, other):

        if type(self) != type(other):
            return False

        else:

            self_kwargs = sorted([key for key in self.__dict__.keys() if key not in type(self)._reservedKws])
            other_kwargs = sorted([key for key in other.__dict__.keys() if key not in type(other)._reservedKws])

            for self_kw, other_kw in zip(self_kwargs, other_kwargs):

                if self_kw != other_kw:
                    return self_kw < other_kw

            else:

                for kw in self_kwargs:

                    if self.__dict__[kw] != other.__dict__[kw]:

                        try:
                            return self.__dict__[kw] < other.__dict__[kw]

                        except TypeError:
                            return True # :shrug:

                else:
                    return False # they are equal

    def __gt__(self, other):

        if type(self) != type(other):
            return False

        else:
            return not(self < other)

    @abstractmethod
    def __hash__(self):pass

    def __eq__(self, other):
        return type(self) == type(other) and self.toJson() == other.toJson()

    def __str__(self):

        if self._str is not None:
            return self._str

        else:

            ret = f"{self.__class__.__name__}("
            ordered = sorted(
                [(key, val) for key, val in self.__dict__.items() if key not in type(self)._reservedKws],
                key = lambda t: t[0]
            )
            ret += ", ".join(f"{key}={repr(val)}" for key, val in ordered)
            ret += ")"
            self._str = ret
            return self._str

    def __repr__(self):
        return str(self)

    def __copy__(self):
        info = type(self)(placeholder = "placeholder")
        del info.placeholder
        info.__dict__.update(self.__dict__)
        return info

    def __deepcopy__(self, memo):
        return self.__copy__()

class ApriInfo(_Info):

    _reservedKws = ["_json", "_hash", "_str"]

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

class AposInfo(_Info):

    def __hash__(self):
        raise TypeError("`Apos_Info` is not a hashable type.")

