import json
from abc import ABC, abstractmethod
from copy import copy, deepcopy

from ._utilities import is_int, order_json_obj

_APRI_ENCODED_PREFIX = "ApriInfo.from_json("
_APRI_ENCODED_SUFFIX = ")"
_APRI_ENCODED_PREFIX_LEN = len(_APRI_ENCODED_PREFIX)
_APRI_ENCODED_SUFFIX_LEN = len(_APRI_ENCODED_SUFFIX)
_APRI_ENCODED_PRE_SUFFIX_LEN = _APRI_ENCODED_PREFIX_LEN + _APRI_ENCODED_SUFFIX_LEN
_APOS_ENCODED_PREFIX = "AposInfo.from_json("
_APOS_ENCODED_SUFFIX = ")"
_APOS_ENCODED_PREFIX_LEN = len(_APOS_ENCODED_PREFIX)
_APOS_ENCODED_SUFFIX_LEN = len(_APOS_ENCODED_SUFFIX)
_APOS_ENCODED_PRE_SUFFIX_LEN = _APOS_ENCODED_PREFIX_LEN + _APOS_ENCODED_SUFFIX_LEN


class JSONDecoderWithRoot(json.JSONDecoder, ABC):

    @abstractmethod
    def decode_root(self, obj):pass


class _InfoJsonEncoder(json.JSONEncoder):

    def default(self, obj):

        if isinstance(obj, ApriInfo):
            return _APRI_ENCODED_PREFIX + obj.to_json() + _APRI_ENCODED_SUFFIX

        elif isinstance(obj, AposInfo):
            return _APOS_ENCODED_PREFIX + obj.to_json() + _APOS_ENCODED_SUFFIX

        elif is_int(obj):
            return int(obj)

        elif isinstance(obj, tuple):
            return list(obj)

        else:
            return super().default(obj)


class _InfoJsonDecoder(JSONDecoderWithRoot):

    def __init__(self, *args, **kwargs):
        super().__init__(object_hook=self.object_hook, *args, **kwargs)

    @staticmethod
    def check_return_apri_info_json(str_):

        check = (
            len(str_) > _APRI_ENCODED_PRE_SUFFIX_LEN and
            str_[ : _APRI_ENCODED_PREFIX_LEN] == _APRI_ENCODED_PREFIX and
            str_[-_APRI_ENCODED_SUFFIX_LEN : ] == _APRI_ENCODED_SUFFIX
        )
        return (
            check,
            str_[_APRI_ENCODED_PREFIX_LEN : -_APRI_ENCODED_SUFFIX_LEN].strip(" \t") if check else None
        )

    @staticmethod
    def check_return_apos_info_json(str_):

        check = (
            len(str_) > _APOS_ENCODED_PRE_SUFFIX_LEN and
            str_[ : _APOS_ENCODED_PREFIX_LEN] == _APOS_ENCODED_PREFIX and
            str_[-_APOS_ENCODED_SUFFIX_LEN : ] == _APOS_ENCODED_SUFFIX
        )
        return (
            check,
            str_[_APOS_ENCODED_PREFIX_LEN : -_APOS_ENCODED_SUFFIX_LEN].strip(" \t") if check else None
        )

    def object_hook(self, obj):

        if isinstance(obj, str):

            obj = obj.strip(" \t")

            check_apri, apri_json = _InfoJsonDecoder.check_return_apri_info_json(obj)
            check_apos, apos_json = _InfoJsonDecoder.check_return_apos_info_json(obj)

            if check_apri:

                try:
                    return ApriInfo(**self.decode(apri_json))

                except json.JSONDecodeError:
                    return obj

            elif check_apos:

                try:
                    return AposInfo(**self.decode(apos_json))

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

    def decode_root(self, obj):

        decoded_json = self.decode(obj)

        if not isinstance(decoded_json, dict):
            raise ValueError(
                "The outermost layer of the passed JSON string must be a JavaScript `object`, that is, " +
                f"a Python `dict`. The outermost layer of the passed `json_string` is: " +
                f"`{decoded_json.__class__.__name__}`."
            )

        return decoded_json


class _Info(ABC):

    _reserved_kws = ["_memoize_json", "_str", "_json", "_encoder"]
    _default_encoder = _InfoJsonEncoder(
        ensure_ascii = True,
        allow_nan = True,
        indent = None,
        separators = (',', ':')
    )
    _default_decoder = _InfoJsonDecoder()

    def __init_subclass__(cls, reserved_kws = None, **kwargs):

        super().__init_subclass__(**kwargs)

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
        self._encoder = _Info._default_encoder

    @classmethod
    def _check_reserved_kws(cls, kwargs):

        if any(kw in kwargs for kw in cls._reserved_kws):
            raise ValueError(
                "The following keyword-argument keys are reserved. Choose a different key.\n" +
                f"{', '.join(cls._reserved_kws)}"
            )

    @classmethod
    def from_json(cls, json_str, decoder = None):

        if decoder is None:
            decoder = cls._default_decoder

        if not isinstance(decoder, JSONDecoderWithRoot):
            raise TypeError("`decoder` must subclass `JSONDecoderWithRoot`.")

        decoded = decoder.decode_root(json_str)
        return cls(**decoded)

    def to_json(self):

        if self._memoize_json and self._json is not None:
            return self._json

        else:

            kwargs = copy(self.__dict__)

            for kw in type(self)._reserved_kws:
                kwargs.pop(kw,None)

            kwargs = order_json_obj(kwargs)

            try:
                json_rep = self._encoder.encode(kwargs)

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

        if not isinstance(mode, str):
            raise TypeError("`mode` must be of type `str`.")

        if mode != "dfs" and mode != "bfs":
            raise ValueError("`mode` must be either 'dfs' or 'bfs'.")

        if mode == "dfs":
            yield from self._iter_inner_info_dfs(True)

        else:
            yield from self._iter_inner_info_bfs(True)

    def change_info(self, old_info, new_info, _root_call = True):

        if not isinstance(old_info, _Info):
            raise TypeError("`old_info` must be of type `_Info`.")

        if not isinstance(new_info, _Info):
            raise TypeError("`new_info` must be of type `_Info`.")

        if not isinstance(_root_call, bool):
            raise TypeError("`_rootCall` must be of type `bool`.")

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

    def set_encoder(self, encoder):

        if not isinstance(encoder, json.JSONEncoder):
            raise TypeError("`encoder` must be of type `json.JSONEncoder`.")

        self._encoder = encoder

    def clean_encoder(self):
        self._encoder = type(self)._default_encoder

    def __iter__(self):

        for key, val in self.__dict__.items():

            if key not in type(self)._reserved_kws:
                yield key, val

    def __contains__(self, apri):

        if not isinstance(apri, ApriInfo):
            raise TypeError("`info` must be of type `ApriInfo`.")

        return any(inner == apri for _, inner in self.iter_inner_info())

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
        del info.placeholder
        info.__dict__.update(self.__dict__)
        return info

    def __deepcopy__(self, memo):
        return self.__copy__()


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
        raise TypeError("`Apos_Info` is not a hashable type.")