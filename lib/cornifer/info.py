import json
from abc import ABC, abstractmethod
from copy import copy, deepcopy

from ._utilities import is_int, order_json_obj, check_type, check_type_None_default

class _InfoJsonEncoder(json.JSONEncoder):

    def default(self, obj):

        if isinstance(obj, _Info):
            return type(obj).__name__ + obj.to_json()

        elif is_int(obj):
            return int(obj)

        elif isinstance(obj, tuple):
            return list(obj)

        else:
            return super().default(obj)

class _Info(ABC):

    _reserved_kws = ["_memoize_json", "_str", "_json", "_default_encoder"]
    _subclasses = []
    _default_encoder = _InfoJsonEncoder(
        ensure_ascii = True,
        allow_nan = True,
        indent = None,
        separators = (',', ':')
    )

    def __init_subclass__(cls, reserved_kws = None, **kwargs):

        super().__init_subclass__(**kwargs)
        _Info._subclasses.append(cls)

        if reserved_kws is None:
            cls._reserved_kws = _Info._reserved_kws

        else:
            cls._reserved_kws = _Info._reserved_kws + reserved_kws

    def __init__(self, **kwargs):

        if len(kwargs) == 0:
            raise ValueError("must pass at least one keyword argument.")

        type(self)._check_reserved_kws(kwargs)
        self.__dict__.update(kwargs)
        self._json = None
        self._str = None
        self._memoize_json = False

    @classmethod
    def _default_str_hook(cls, str_):

        if str_[ : len(cls.__name__)] != cls.__name__:
            return str_

        try:
            decoded = json.JSONDecoder().decode(str_[len(cls.__name__) : ])

        except json.JSONDecodeError:
            return str_

        else:

            if isinstance(decoded, dict):
                return decoded

            else:
                return str_

    @classmethod
    def _check_reserved_kws(cls, kwargs):

        if any(kw in kwargs for kw in cls._reserved_kws):
            raise ValueError(
                "The following keyword-argument keys are reserved. Choose a different key.\n" +
                f"{', '.join(cls._reserved_kws)}"
            )

    @classmethod
    def from_json(cls, json_, str_hook = None):

        check_type(json_, "json_", str)

        if str_hook is None:
            str_hook = lambda cls_, str_ : cls_._default_str_hook(str_)

        info_decoded_json = json.JSONDecoder().decode(json_)

        if not isinstance(info_decoded_json, dict):
            raise ValueError(
                "The outermost layer of the passed JSON string must be a JavaScript `object`, that is, "
                f"a Python `dict`. The outermost layer of the passed `json_` is: "
                f"`{info_decoded_json.__class__.__name__}`."
            )

        stack = [(None, None, cls, info_decoded_json)]

        while len(stack) > 0:

            index = len(stack) - 1
            current = stack[-1]
            old_index, old_key, current_cls, current_info_decoded_json = current

            if isinstance(current_info_decoded_json, str):
                raise Exception

            for key, val in current_info_decoded_json.items():

                if isinstance(val, str):

                    str_ = val.strip(" \t")

                    for cls_ in _Info._subclasses:

                        decoded_str = str_hook(cls_, str_)

                        if isinstance(decoded_str, dict):

                            stack.append((index, key, cls_, decoded_str))
                            current_info_decoded_json[key] = decoded_str
                            break # cls_ loop

                elif isinstance(val, list):
                    current_info_decoded_json[key] = tuple(val)

            if index + 1 == len(stack):
                # nothing pushed
                if index == 0:
                    # top level
                    return current_cls(**current_info_decoded_json)
                else:
                    # pop
                    old_info_decoded_json = stack[old_index][3]
                    old_info_decoded_json[old_key] = current_cls(**current_info_decoded_json)
                    del stack[-1]


    def to_json(self, encoder = None):

        encoder = check_type_None_default(encoder, "encoder", _InfoJsonEncoder, type(self)._default_encoder)

        if self._memoize_json and self._json is not None:
            return self._json

        else:

            kwargs = copy(self.__dict__)

            for kw in type(self)._reserved_kws:
                kwargs.pop(kw, None)

            kwargs = order_json_obj(kwargs)

            try:
                json_rep = encoder.encode(kwargs)

            except (TypeError, ValueError) as e:

                raise ValueError(
                    "One of the keyword arguments used to construct this instance cannot be encoded into " +
                    "JSON. Use different keyword arguments, or override the " +
                    f"classmethod `{self.__class__.__name__}.from_json` and the instancemethod " +
                    f"`{self.__class__.__name__}.toJson`."
                ) from e

            if "\0" in json_rep:

                raise ValueError(
                    "One of the keyword arguments used to construct this instance contains the null character " +
                    "'\\0'."
                )

            if self._memoize_json:
                self._json = json_rep

            return json_rep

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

        if (
            self._memoize_json and other._memoize_json and
            self._json is not None and other._json is not None
        ):
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

    def __getattr__(self, item):
        raise AttributeError(f'{item} is not an attribute of {self}') from None

    def __setattr__(self, key, value):

        if key in type(self)._reserved_kws:
            self.__dict__[key] = value

        else:
            raise AttributeError(f'Attributes of ApriInfo are readonly (cannot be changed).')

    def __delattr__(self, item):

        if item not in type(self)._reserved_kws:
            raise AttributeError(f'Attributes of ApriInfo are readonly (cannot be deleted).')

class ApriInfo(_Info, reserved_kws = ["_hash"]):

    def __init__(self, **kwargs):

        super().__init__(**kwargs)
        hash_ = hash(type(self))
        self._memoize_json = True

        for key,val in kwargs.items():

            try:
                hash_ += hash(val)

            except (TypeError, AttributeError) as e:

                raise ValueError(
                    f"All keyword arguments must be hashable types. The keyword argument given by \"{key}\" "+
                    f"not a hashable type. The type of that argument is `{val.__class__.__name__}`."
                ) from e

            if self._memoize_json and isinstance(val, _Info):
                self._memoize_json = False

        self._hash = hash_

    def __hash__(self):
        return self._hash

class AposInfo(_Info):

    def __hash__(self):
        raise TypeError(f"`{type(self).__name__}` is not a hashable type.")
