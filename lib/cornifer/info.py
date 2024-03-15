from abc import ABC, abstractmethod
from copy import copy, deepcopy

from ._utilities import order_json, check_type, JSONEncodable, default_encoder

_INFO_TYPE_KW = '_Info_type'

class _Info(ABC, JSONEncodable):

    _reserved_kws = ["_str", _INFO_TYPE_KW, '_json']
    _subclasses = {}

    def __init_subclass__(cls, reserved_kws = None, **kwargs):

        super().__init_subclass__(**kwargs)
        _Info._subclasses[cls.__name__] = cls

        if reserved_kws is None:
            cls._reserved_kws = _Info._reserved_kws

        else:
            cls._reserved_kws = _Info._reserved_kws + reserved_kws

    def __init__(self, **kwargs):

        type(self)._check_reserved_kws(kwargs)
        self.__dict__.update(kwargs)
        self._str = None
        self._json = None

    def __getstate__(self):
        return {key: val for key, val in self.__dict__.items() if key not in type(self)._reserved_kws}

    def __setstate__(self, state):

        for key, val in state.items():
            self.__dict__[key] = val

    @classmethod
    def _check_reserved_kws(cls, kwargs):

        if any(kw in kwargs for kw in cls._reserved_kws):
            raise ValueError(
                "The following keyword-argument keys are reserved. Choose a different key. "
                f"{', '.join(cls._reserved_kws)}"
            )

    @classmethod
    def from_primitive_json(cls, json_):

        if json_[_INFO_TYPE_KW] != cls.__name__:
            raise ValueError

        json_ = copy(json_)
        del json_[_INFO_TYPE_KW]
        return cls(**json_)

    @staticmethod
    def from_json(json_):

        check_type(json_, "json", dict)
        # I can't use object_hook because it's bugged (that or py-lmdb is bugged, or both)
        if not isinstance(json_, dict):
            raise TypeError

        if _INFO_TYPE_KW not in json_.keys():
            raise KeyError

        stack = [(None, None, json_)]

        while len(stack) > 0:

            index = len(stack) - 1
            current = stack[-1]
            old_index, old_key, curr_json_obj = current

            for key, val in curr_json_obj.items():

                if isinstance(val, dict):
                    stack.append((index, key, val))

                elif isinstance(val, list):
                    curr_json_obj[key] = tuple(val)

            if index + 1 == len(stack):
                # nothing pushed
                info = _Info._subclasses[curr_json_obj[_INFO_TYPE_KW]].from_primitive_json(curr_json_obj)
                if index == 0: # top level
                    return info
                else:
                    # pop
                    old_json_obj = stack[old_index][2]
                    old_json_obj[old_key] = info
                    del stack[-1]

    def to_json(self, *args):

        if self._json is None:
            self._json = default_encoder.encode(self)

        return self._json

    def json_encode_default(self):

        kwargs = copy(self.__dict__)

        for kw in type(self)._reserved_kws:
            kwargs.pop(kw, None)

        json_ = {key : val for key, val in self.__dict__.items() if key not in type(self)._reserved_kws}
        json_['_Info_type'] = str(type(self))
        return order_json(json_)

    def _iter_inner_info_bfs(self, root_call):

        if root_call:
            yield None, self

        subinfos = []

        for key, val in self.__dict__.items():

            if key not in type(self)._reserved_kws and isinstance(val, _Info):

                subinfos.append((key, val))
                yield key, val

        for key, val in subinfos:
            yield from val._iter_inner_info_bfs(False)

    def _iter_inner_info_dfs(self, root_call):

        for key, val in self.__dict__.items():

            if key not in type(self)._reserved_kws and isinstance(val, _Info):

                yield from val._iter_inner_info_dfs(False)
                yield key, val

        if root_call:
            yield None, self

    def iter_inner_info(self, mode = "dfs"):
        """Iterate over `_Info` contained within this `_Info`.

        :param mode: (type `str`) Optional, default "dfs". Whether to return depth- (dfs) or breadth-first (bfs).
        :return: (type `str`) Keyword names, or `None` for the root.
        :return: (type `_Info`) Keyword values, `self` for the root.
        """

        check_type(mode, "mode", str)

        if mode != "dfs" and mode != "bfs":
            raise ValueError("`mode` must be either 'dfs' or 'bfs'.")

        if mode == "dfs":
            yield from self._iter_inner_info_dfs(True)

        else:
            yield from self._iter_inner_info_bfs(True)

    def change_info(self, old_info, new_info, _root_call = True):

        check_type(old_info, "old_info", _Info)
        check_type(new_info, "new_info", _Info)

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

    def __iter__(self):

        for key, val in self.__dict__.items():

            if key not in type(self)._reserved_kws:
                yield key, val

    def __contains__(self, info):

        check_type(info, "info", _Info)
        return any(inner == info for _, inner in self.iter_inner_info())

    def __lt__(self, other):

        if type(self) != type(other):
            return False

        else:

            self_kwargs = sorted([key for key in self.__dict__.keys() if key not in type(self)._reserved_kws])
            other_kwargs = sorted([key for key in other.__dict__.keys() if key not in type(other)._reserved_kws])

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

        if type(self) != type(other):
            return False

        if self._json is not None and other._json is not None:
            return self._json == other._json

        if len(self.__dict__) != len(other.__dict__):
            return False

        try:

            if hash(self) != hash(other):
                return False

        except TypeError:
            pass

        for key, val in self.__dict__.items():

            if key not in type(self)._reserved_kws:

                if key not in other.__dict__.keys():
                    return False

                elif val != other.__dict__[key]:
                    return False

        else:
            return True

    def __str__(self):

        if self._str is not None:
            return self._str

        else:

            ret = f"{self.__class__.__name__}("
            ordered = sorted(
                [(key, val) for key, val in self.__dict__.items() if key not in type(self)._reserved_kws],
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
        del info.__dict__['placeholder']
        info.__dict__.update(self.__dict__)
        return info

    def __deepcopy__(self, memo):
        return self.__copy__()

class ApriInfo(_Info, reserved_kws = ["_hash"]):

    def __init__(self, **kwargs):

        if len(kwargs) == 0:
            raise ValueError("must pass at least one keyword argument.")

        super().__init__(**kwargs)
        hash_ = hash(type(self))

        for key,val in kwargs.items():

            try:
                hash_ += hash(val)

            except (TypeError, AttributeError) as e:

                raise ValueError(
                    f"All keyword arguments must be hashable types. The keyword argument given by {key} is"
                    f"not a hashable type. The type of that argument is {val.__class__.__name__}."
                ) from e

        self._hash = hash_

    def __hash__(self):
        return self._hash

    def __setattr__(self, key, value):

        if key in type(self)._reserved_kws:
            self.__dict__[key] = value

        else:
            raise AttributeError(f'Attributes of ApriInfo are readonly (cannot be changed).')

    def __delattr__(self, item):

        if item not in type(self)._reserved_kws:
            raise AttributeError(f'Attributes of ApriInfo are readonly (cannot be deleted).')

        else:
            del self.__dict__[item]

class AposInfo(_Info):

    def __hash__(self):
        raise TypeError(f"{type(self).__name__} is not a hashable type.")