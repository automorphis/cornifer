import functools
import warnings
from abc import ABC, abstractmethod
from contextlib import ExitStack
from pathlib import Path

from .info import ApriInfo
from .blocks import Block
from ._utilities import check_type, random_unique_filename, intervals_overlap, timeout_cm, check_return_int, \
    check_return_Path, check_return_int_None_default, check_return_Path_None_default, check_type_None_default
from ._transactions import Writer, ReversibleWriter, StagingReader
from ._regstatics import get_apri_id_key, _IS_NOT_COMPRESSED_VAL
from .errors import DataExistsError, RegisterRecoveryError, DataNotFoundError, ReturnNotReadyError
from .filemetadata import FileMetadata

_debug = 0

class Return:

    def __init__(self):

        self._value = None
        self._ready = False

    @property
    def value(self):

        if self._ready:
            return self._value

        else:
            raise ReturnNotReadyError

    @value.setter
    def value(self, val):

        self._value = val
        self._ready = True

class _BaseRegisterMethod(ABC):

    method_name = arg_names = arg_types = num_required_args = default_args = recurable = generator = None

    def __init__(self, reg, args, kwargs):

        self._reg = reg
        self._args = args
        self._ret = Return()
        cls = type(self)
        num_positional = len(args)
        default_args = cls.default_args[ num_positional - cls.num_required_args : ] # len is len(names) - num_positional

        if num_positional < cls.num_required_args:
            raise ValueError(
                f'The method `{type(self).method_name}` takes {cls.num_required_args} required positional arguments: '
                ', '.join(cls.arg_names[ : cls.num_required_args])
            )

        for i, (name, arg) in enumerate(zip(cls.arg_names, args + default_args)):

            if name in kwargs.keys():

                if i < num_positional:
                    raise ValueError(
                        f'Parameter `{name}` passed to `{type(self).method_name}` as both a keyword and positional '
                        f'argument.'
                    )

                else:
                    arg = kwargs.pop(name)

            setattr(self, name, arg)

        self._kwargs = kwargs

        try:
            self._timeout = self.timeout

        except AttributeError:
            self._timeout = None

    def __init_subclass__(cls, **kwargs):

        cls.method_name = kwargs.pop('method_name')
        cls.arg_names = kwargs.pop('arg_names')
        cls.arg_types = kwargs.pop('arg_types')
        cls.num_required_args = kwargs.pop('num_required_args')
        cls.default_args = kwargs.pop('default_args')
        cls.recurable = kwargs.pop('recurable')
        cls.generator = kwargs.pop('generator')
        setattr(
            RegisterTransaction,
            cls.method_name,
            functools.partialmethod(RegisterTransaction.init_and_push, cls)
        )

        if not len(cls.arg_names) == cls.num_required_args + len(cls.default_args):
            raise ValueError

        super().__init_subclass__(**kwargs)

    def __call__(self, sro_txn, rrw_txn, rw_txn):

        with RegisterTransaction(self._reg, self._timeout, sro_txn, rrw_txn, rw_txn) as txn:
            txn.push(self)

        return self._ret.value

    @abstractmethod
    def type_value(self):

        self._reg._check_open_raise(type(self).method_name)
        cls = type(self)

        for i, (name, type_, default) in enumerate(zip(cls.arg_names, cls.arg_types, cls.default_args)):

            arg = getattr(self, name)

            if i < cls.num_required_args:

                if issubclass(type_, int):
                    setattr(self, name, check_return_int(arg, name))

                elif issubclass(type_, Path):
                    setattr(self, name, check_return_Path(arg, name))

                elif type_ is not None:
                    check_type(arg, name, type_)

            else:

                default = cls.default_args[i - cls.num_required_args]

                if issubclass(type_, int):

                    if default is None:
                        setattr(self, name, check_return_int_None_default(arg, name, default))

                    else:
                        setattr(self, name, check_return_int(arg, name))

                elif issubclass(type_, Path):

                    if default is None:
                        setattr(self, name, check_return_Path_None_default(arg, name, default))

                    else:
                        setattr(self, name, check_return_Path(arg, name))

                elif type_ is not None:

                    if default is None:
                        check_type_None_default(arg, name, type_, default)

                    else:
                        check_type(arg, name, type_)

    @abstractmethod
    def ram(self):
        pass

    @abstractmethod
    def pre(self, r_txn):
        pass

    @abstractmethod
    def disk1(self, rw_txn):
        pass

    @abstractmethod
    def disk2(self):
        pass

    def recursive_gen(self, r_txn):

        if not type(self).recurable:
            raise TypeError

        for subreg, ro_txn in self._reg._subregs_bfs(True, r_txn):

            method = type(self)(subreg, self._args, self._kwargs)
            method.recursively = False
            sro_txn = ro_txn.cast(StagingReader)
            yield from method(sro_txn, None, None)

    def recursive_non_gen_shortcircuit(self, ret):
        return True

    def recursive_non_gen_reduce(self, ret, new_ret):
        return new_ret

    def recursive_non_gen(self, r_txn):

        if type(self).recurable:
            raise TypeError

        ret = None

        for subreg, ro_txn in self._reg._subregs_bfs(True, r_txn):

            method = type(self)(subreg, self._args, self._kwargs)
            method.recursively = False
            sro_txn = ro_txn.cast(StagingReader)

            try:
                new_ret = method(sro_txn, None, None)

            except DataNotFoundError:
                pass

            else:

                ret = self.recursive_non_gen_reduce(ret, new_ret)

                if self.recursive_non_gen_shortcircuit(ret):
                    return ret

    @abstractmethod
    def ret(self):
        pass

    @abstractmethod
    def error(self, rrw_txn, rw_txn, e):
        pass

class RegisterTransaction:

    def __init__(self, reg, timeout, sro_txn, rrw_txn, rw_txn):

        self._reg = reg
        self._timeout = timeout
        self._sro_txn = sro_txn
        self._rrw_txn = rrw_txn
        self._rw_txn = rw_txn
        self._rets = None
        self._stopwatch = None
        self._methods = None
        self._sro_txn = None
        self._stack = None

    def __enter__(self):

        self._methods = []

        with timeout_cm(self._timeout) as (cm, self._stopwatch):

            with ExitStack() as stack:

                if self._sro_txn is None:
                    self._sro_txn = stack.enter_context(self._reg._txn(StagingReader))

                self._stack = stack.pop_all()

        return self

    def __exit__(self, *args):

        with timeout_cm(self._timeout, self._stopwatch) as (cm, _):

            self._stack.__exit__(*args)
            empty_stage = self._sro_txn.empty_stage()

            if not empty_stage:

                with ExitStack() as stack:

                    if self._rrw_txn is None:
                        self._rrw_txn = stack.enter_context(self._reg._txn(ReversibleWriter))

                    self._sro_txn.commit_stage(self._rrw_txn)

            try:

                for method in self._methods:

                    method.disk2()
                    method._ret.value = method.ret()

            except BaseException as e:

                cm.cancel()

                with ExitStack() as stack:

                    if not empty_stage:

                        if self._rw_txn is None:
                            self._rw_txn = stack.enter_context(self._reg._txn(Writer))

                    else:
                        self._rw_txn = None

                    for method in self._methods:
                        ee = method.error(self._rrw_txn, self._rw_txn, e)

                raise ee

    def init_and_push(self, method, *args, **kwargs):
        self.push(method(self._reg, args, kwargs))

    def push(self, method):

        self._methods.append(method)
        method.type_value()
        method.ram()
        method.pre(self._sro_txn)
        method.disk1(self._sro_txn)

##################################################################
#                       WRITE METHODS                            #
##################################################################

class RegisterWriteMethod(
    _BaseRegisterMethod, ABC,
    recurable = False,
    generator = False
):

    @abstractmethod
    def type_value(self):

        super().type_value()
        self._reg._check_readwrite_raise(type(self).method_name)

class AddDiskBlk(
    RegisterWriteMethod,
    method_name = 'add_disk_blk',
    arg_names = ('blk', 'exists_ok', 'dups_ok', 'ret_metadata', 'timeout'),
    arg_types = (Block, bool, bool, bool, int),
    num_required_args = 1,
    default_args = (False, True, False, None)
):

    def __init__(self, reg, args, kwargs):

        self._apri_json = self._blk_key = self._compressed_key = self._filename = self._add_apri = None
        super().__init__(reg, args, kwargs)

    def type_value(self):

        super().type_value()

        if len(self.blk) > self._reg._max_length:
            raise ValueError

        startn_head = self.blk.startn() // self._reg._startn_tail_mod

        if startn_head != self._reg.startn_head:
            raise IndexError(
                'The `startn` for the passed `Block` does not have the correct head:\n'
                f'`tail_len`      : {self._reg._startn_tail_length}\n'
                f'expected `head` : {self._reg._startn_head}\n'
                f'`startn`        : {self.blk.startn()}\n'
                f'`startn` head   : {startn_head}\n'
                'Please see the method `set_startn_info` to troubleshoot this error.'
            )

    def pre(self, r_txn):

        try:
            self._apri_json = self._reg._relational_encode_info(self.blk.apri(), r_txn)

        except DataNotFoundError:
            self._add_apri = True

        else:

            apri_id_key = get_apri_id_key(self._apri_json)
            self._add_apri = not r_txn.has_key(apri_id_key)

        self._filename = random_unique_filename(self._reg._local_dir, suffix = type(self._reg).file_suffix, length = 6)
        apri = self.blk.apri()
        startn = self.blk.startn()
        length = len(self.blk)

        if not self._add_apri:

            self._blk_key, self._compressed_key = self._reg._get_disk_blk_keys(
                apri, self._apri_json, False, startn, length, r_txn
            )

            if not self.exists_ok and r_txn.has_key(self._blk_key):
                raise DataExistsError(
                    f'Duplicate `Block` with the following data already exists in this `Register`: {apri}, startn = '
                    f'{startn}, length = {length}.'
                )

            if not self.dups_ok:

                prefix = self._reg._intervals_pre(apri, self._apri_json, False, r_txn)
                int1 = (startn, length)

                for int2 in self._reg._intervals_disk(prefix, r_txn):

                    if intervals_overlap(int1, int2):
                        raise DataExistsError(
                            'Attempted to add a `Block` with duplicate indices. Set `dups_ok` to `True` to suppress.'
                        )

        else:
            self._blk_key = self._compressed_key = None

        if length == 0:
            warnings.warn(f'Added a length 0 disk1 `Block`.\n{apri}\nstartn = {startn}\n{self._reg}')

    def disk1(self, rrw_txn):

        apri = self.blk.apri()
        startn = self.blk.startn()
        length = len(self.blk)

        if self._add_apri:

            self._reg._add_apri_disk(apri, [], False, rrw_txn)
            self._blk_key, self._compressed_key = self._reg._get_disk_blk_keys(
                apri, None, True, startn, length, rrw_txn
            )

        filename_bytes = self._filename.name.encode('ASCII')
        rrw_txn.put(self._blk_key, filename_bytes)
        rrw_txn.put(self._compressed_key, _IS_NOT_COMPRESSED_VAL)

    def disk2(self):

        if _debug == 1:
            raise KeyboardInterrupt

        type(self._reg).dump_disk_data(self.blk.segment(), self._filename, **self._kwargs)

        if _debug == 2:
            raise KeyboardInterrupt

        if self.ret_metadata:
            return FileMetadata.from_path(self._filename)

        else:
            return None

    def error(self, rrw_txn, rw_txn, e):

        if isinstance(e, RegisterRecoveryError):
            return e

        try:

            if self._filename is not None:

                try:
                    self._filename.unlink()

                except FileNotFoundError:
                    pass

            rrw_txn.reverse(rw_txn)

        except BaseException as ee:

            eee = RegisterRecoveryError('Could not successfully recover from a failed disk1 `Block` add!')
            eee.__cause__ = ee
            return eee

        else:
            return e

    def ret(self):
        pass

class AppendDiskBlk(
    AddDiskBlk,
    method_name = 'append_disk_blk',
    arg_names = ('blk', 'ret_metadata', 'timeout'),
    arg_types = (Block, bool, int),
    num_required_args = 1,
    default_args = (False, None)
):

    def pre(self, r_txn):

        apri = self.blk.apri()
        startn = self.blk.startn()
        length = len(self.blk)

        try:
            self._apri_json = self._reg._relational_encode_info(apri, r_txn)

        except DataNotFoundError:
            self._add_apri = True

        else:

            apri_id_key = get_apri_id_key(self._apri_json)
            self._add_apri = not r_txn.has_key(apri_id_key)

        self._filename = random_unique_filename(self._reg._local_dir, suffix = type(self._reg).file_suffix, length = 6)

        if not self._add_apri:

            try:
                prefix = self._reg._maxn_pre(apri, self._apri_json, False, r_txn)

            except DataNotFoundError: # if apri has no disk1 blks (used passed startn)
                pass

            else:
                startn = self._reg._maxn_disk(prefix, r_txn) + 1

            self._blk_key, self._compressed_key = self._reg._get_disk_blk_keys(
                apri, self._apri_json, False, startn, length, r_txn
            )

        else:
            self._blk_key = self._compressed_key = None

        if length == 0:
            warnings.warn(f'Added a length 0 disk1 `Block`.\n{apri}\nstartn = {startn}\n{self._reg}')

class RmvDiskBlk(
    RegisterWriteMethod,
    method_name = 'rmv_disk_blk',
    arg_names = ('apri', 'startn', 'length', 'missing_ok', 'timeout'),
    arg_types = (ApriInfo, int, int, bool, int),
    num_required_args = 1,
    default_args = (None, None, False, None)
):

    def type_value(self):
        pass

    def pre(self, r_txn):
        pass

    def disk1(self, rrw_txn):
        pass

    def disk2(self):
        pass

    def ret(self):
        pass

    def error(self, rrw_txn, rw_txn, e):
        pass

class Compress(RegisterWriteMethod):
    pass

class Decompress(RegisterWriteMethod):
    pass

class ChangeApri(RegisterWriteMethod):
    pass

class RmvApri(RegisterWriteMethod):
    pass

class SetMsg(RegisterWriteMethod):
    pass

class SetShorthand(RegisterWriteMethod):
    pass

class SetStartnInfo(RegisterWriteMethod):
    pass

class IncreaseSize(RegisterWriteMethod):
    pass

class AddSubreg(RegisterWriteMethod):
    pass

class RmvSubreg(RegisterWriteMethod):
    pass

class SetApos(RegisterWriteMethod):
    pass

class RmvApos(RegisterWriteMethod):
    pass

##################################################################
#                        READ METHODS                            #
##################################################################

class RegisterReadMethod(_BaseRegisterMethod, ABC):

    def __init_subclass__(cls, **kwargs):

        cls.recurable = kwargs.pop('recurable')
        cls.generator = kwargs.pop('generator')
        super().__init_subclass__(**kwargs)

    def error(self, rrw_txn, rw_txn, e):
        return e