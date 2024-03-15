import functools
import itertools
import shutil
import tempfile
import warnings
import zipfile
from abc import ABC, abstractmethod
from contextlib import ExitStack
from pathlib import Path

from ._relationalinfo import RelApriInfo
from .info import ApriInfo
from .blocks import Block
from ._utilities import check_type, random_unique_filename, intervals_overlap, timeout_cm, check_return_int, \
    check_return_Path, check_return_int_None_default, check_return_Path_None_default, check_type_None_default, \
    is_deletable, interval_comparator, check_iterable, check_return_iterable_None_default, combine_intervals, bsenc, \
    bytify_int, intify_bytes, bsdec
from ._transactions import Writer, ReversibleWriter, StagingReader
from ._regstatics import get_apri_id_key, IS_NOT_COMPRESSED_VAL, disk_blk_keys_exist, ID_APRI_KEY_PREFIX, \
    BLK_KEY_PREFIX_LEN, get_apri_id, KEY_SEP, BLK_KEY_PREFIX, COMPRESSED_KEY_PREFIX, KEY_SEP_LEN, \
    COMPRESSED_KEY_PREFIX_LEN
from .errors import DataExistsError, RegisterRecoveryError, DataNotFoundError, ReturnNotReadyError, CompressionError, \
    DecompressionError, BlockNotOpenError, RegisterError
from .filemetadata import FileMetadata

_debug = 0

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

                    if not method._ret_after_ram:

                        method.disk2()
                        method._ret_obj.value = method.ret()

            except BaseException as e:

                cm.cancel()

                with ExitStack() as stack:

                    if not empty_stage:

                        if self._rw_txn is None:
                            self._rw_txn = stack.enter_context(self._reg._txn(Writer))

                    else:
                        self._rw_txn = None

                    for method in reversed(self._methods):

                        if not method._ret_after_ram:
                            ee = method.error(self._rrw_txn, self._rw_txn, e)

                raise ee

    def init_and_push(self, method, *args, **kwargs):
        return self.push(method(self._reg, args, kwargs))

    def push(self, method):

        self._methods.append(method)
        method.type_value()
        method.ram()

        if method._ret_after_ram:

            method._ret_obj.value = method.ram()
            return method._ret_obj

        method.disk1(self._sro_txn)
        return method._ret_obj

class Return:

    def __init__(self, value = None, ready = False):

        self._value = value
        self._ready = ready

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

class RegisterMethod(ABC):

    method_name = arg_names = arg_types = num_required_args = default_args = generator = None

    def __init__(self, reg, *args, **kwargs):

        self._reg = reg
        self._args = args
        self._ret_obj = Return()
        cls = type(self)

        if cls.generator:
            self._ret = iter(())

        else:
            self._ret = None

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
        cls.generator = kwargs.pop('generator')

        if not len(cls.arg_names) == cls.num_required_args + len(cls.default_args):
            raise ValueError

        super().__init_subclass__(**kwargs)

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

                elif type_ == check_iterable:

                    if default is None:
                        setattr(self, name, check_return_iterable_None_default(arg, name, default))

                    else:

                        check_iterable(arg, name)
                        setattr(self, name, arg)

                elif type_ is not None:

                    if default is None:
                        check_type_None_default(arg, name, type_, default)

                    else:
                        check_type(arg, name, type_)

    @abstractmethod
    def __call__(self, *args):
        pass

    def ret(self):
        return self._ret

class ProtectedRegisterMethod(
    RegisterMethod,
    method_name = None,
    default_args = (),
    recurable = False,
    generator = False
):

    def __init_subclass__(cls, **kwargs):

        kwargs['num_required_args'] = len(kwargs['arg_names'])
        super().__init_subclass__(**kwargs)

    def type_value(self):
        return self

    def __call__(self, *args):
        raise TypeError

class RegisterDatabaseMethod(RegisterMethod, ABC):

    recurable = None

    def __init__(self, reg, *args, **kwargs):

        super().__init__(reg, *args, **kwargs)
        self._ret_after_ram = False

    def __init_subclass__(cls, **kwargs):

        cls.recurable = kwargs.pop('recurable')
        setattr(
            RegisterTransaction,
            cls.method_name,
            functools.partialmethod(RegisterTransaction.init_and_push, cls)
        )
        super().__init_subclass__(**kwargs)

    def __call__(self, sro_txn, rrw_txn, rw_txn):

        with RegisterTransaction(self._reg, self._timeout, sro_txn, rrw_txn, rw_txn) as txn:
            txn.push(self)

        return self._ret_obj.value

    @abstractmethod
    def ram(self):
        pass

    @abstractmethod
    def disk1(self, sro_txn):
        pass

    @abstractmethod
    def disk2(self):
        pass

    def recursive_gen(self, r_txn):

        if not type(self).recurable:
            raise TypeError

        for subreg, ro_txn in self._reg._subregs_bfs(True, r_txn):

            method = type(self)(subreg, *self._args, **self._kwargs)
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

            method = type(self)(subreg, *self._args, **self._kwargs)
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
    def error(self, rrw_txn, rw_txn, e):
        pass

class RegisterReadMethod(RegisterDatabaseMethod, ABC):

    generator = None

    def __init_subclass__(cls, **kwargs):

        cls.generator = kwargs.pop('generator')

        if cls.recurable:

            if 'recursively' not in cls.arg_names:
                raise NameError

            elif not issubclass(cls.arg_types[cls.arg_names.index('recursively')], bool):
                raise TypeError

        super().__init_subclass__(**kwargs)

    def error(self, rrw_txn, rw_txn, e):
        return e

class SortableRecurableRegisterReadMethod(
    RegisterReadMethod, ABC,
    recurable = True,
    generator = True
):

    key = None

    def __init_subclass__(cls, **kwargs):

        super().__init_subclass__(**kwargs)
        cls.key = kwargs.pop('key')

        if not 'sort' in cls.arg_names:
            raise NameError

        elif not issubclass(cls.arg_types[cls.arg_names.index('sort')], bool):
            raise TypeError

    def recursive_gen(self, r_txn):

        if not self.sort:
            yield from super().recursive_gen(r_txn)

        else:

            yield_from = []

            for subreg, ro_txn in self._reg._subregs_bfs(True, r_txn):

                method = type(self)(subreg, *self._args, **self._kwargs)
                method.recursively = False
                method.sort = False
                sro_txn = ro_txn.cast(StagingReader)
                yield_from.extend(method(sro_txn, None, None))

            yield from sorted(yield_from, key = type(self).key)

    def ret(self):

        if not self.sort:
            return super().ret()

        else:
            return sorted(list(self._ret), key = type(self).key)


##################################################################
#                       ERROR MESSAGES                           #
##################################################################

class RamBlkNotOpenErrMsg(
    ProtectedRegisterMethod,
    arg_names = ('apri', 'startn'),
    arg_types = (ApriInfo, int)
):

    def ret(self):

        self._ret = (
            'Closed RAM Block with the following data (it is good practice to always keep all RAM `Block`s open) :\n'
            f'apri   = {self.apri}\n'
            f'startn  = {self.startn}\n'
            f'{self._reg}'
        )
        return super().ret()

class BlkNotFoundErrMsg(
    ProtectedRegisterMethod,
    arg_names = ('apri', 'startn', 'length', 'n'),
    arg_types = (ApriInfo, int, int, int, bool)
):

    def ret(self):

        self._ret = f'No Block found :\napri = {self.apri}\n'

        if self.n is not None:
            self._ret = f'{self._ret}\nn = {self.n}'

        elif self.startn is not None and self.length is None:
            self._ret = f'{self._ret}\nstartn = {self.startn}'

        elif self.startn is not None and self.length is not None:
            self._ret = f'{self._ret}\nstartn = {self.startn}\nlength = {self.length}'

        self._ret += str(self._reg)
        return super().ret()

class NoApriErrMsg(
    ProtectedRegisterMethod,
    arg_names = ('apri',),
    arg_types = (ApriInfo,)
):

    def ret(self):

        self._ret = f'Unknown ApriInfo :\n{self.apri}\n{self._reg}'
        return super().ret()

class NoAposErrMsg(
    ProtectedRegisterMethod,
    arg_names = ('apri',),
    arg_types = (ApriInfo,),
):

    def ret(self):

        self._ret = f'No AposInfo associated with ApriInfo :\n{self.apri}\n{self._reg}'
        return super().ret()

##################################################################
#                     PROTECTED METHODS                          #
##################################################################

class RawStartnLength(
    ProtectedRegisterMethod,
    arg_names = ('prefix_len', 'key'),
    arg_types = (int, bytes)
):

    def ret(self):

        stop1 = self.prefix_len + self._reg._max_apri_len
        stop2 = stop1 + KEY_SEP_LEN + self._reg._startn_tail_length
        self._ret = (
            self.key[self.prefix_len     : stop1],  # apri id
            self.key[stop1 + KEY_SEP_LEN : stop2],  # startn
            self.key[stop2 + KEY_SEP_LEN : ] # op_length
        )
        return super().ret()

class StartnLength(RawStartnLength):

    def ret(self):

        _, startn_bytes, op_length_bytes = super().ret()
        self._ret = (
            intify_bytes(startn_bytes) + self._reg._startn_head * self._reg._startn_tail_mod,
            self._reg._max_length - intify_bytes(op_length_bytes)
        )
        return self._ret

class DiskBlkKeys(
    RegisterReadMethod, ProtectedRegisterMethod,
    arg_names = ('apri', 'startn', 'length'),
    arg_types = (ApriInfo, int, int)
):

    def __init__(self, reg, *args, **kwargs):

        super().__init__(reg, *args, **kwargs)
        self._apri_id = None

    def ram(self):
        return self

    def disk1(self, sro_txn):

        self._apri_id = get_apri_id(bsenc(self.apri.to_json(sro_txn)), sro_txn)
        return self

    def disk2(self):
        return self

    def ret(self):

        tail = bytify_int(self.startn % self._reg._startn_tail_mod, self._reg._startn_tail_length)
        op_length = bytify_int(self._reg._max_length - self.length, self._reg._length_length)
        suffix = self._apri_id + KEY_SEP + tail + KEY_SEP + op_length
        self._ret = BLK_KEY_PREFIX + suffix, COMPRESSED_KEY_PREFIX + suffix
        return super().ret()

class DiskBlkPrefixes(
    DiskBlkKeys,
    arg_names = ('apri',),
    arg_types = (ApriInfo,)
):

    def ret(self):

        self.startn = 0
        self.length = 1
        blk_key, compressed_key = super().ret()
        len1 = BLK_KEY_PREFIX_LEN + self._reg._max_apri_len + KEY_SEP_LEN
        len2 = COMPRESSED_KEY_PREFIX_LEN + self._reg._max_apri_len + KEY_SEP_LEN
        self._ret = (blk_key[ : len1], compressed_key[ : len2])
        return self._ret

class DiskBlkPrefixesStartn(
    DiskBlkKeys,
    arg_names = ('apri', 'startn'),
    arg_types = (ApriInfo, int)
):

    def ret(self):

        self.length = 1
        blk_key, compressed_key = super().ret()
        len1 = BLK_KEY_PREFIX_LEN + self._reg._max_apri_len + KEY_SEP_LEN + self._reg._startn_tail_length + KEY_SEP_LEN
        len2 = COMPRESSED_KEY_PREFIX_LEN + self._reg._max_apri_len + KEY_SEP_LEN + self._reg._startn_tail_length + KEY_SEP_LEN
        self._ret = (blk_key[: len1], compressed_key[: len2])
        return self._ret

class ResolveStartnLength(
    RegisterReadMethod, ProtectedRegisterMethod,
    arg_names = ('apri', 'startn', 'length'),
    arg_types = (ApriInfo, int, int)
):

    def ram(self):

        if self.startn is not None and self.length is not None:
            self._ret = (self.startn, self.length)

        elif __Contains__(self._reg, self.apri).ram().ret() and NumBlks(self._reg, self.apri).ram().ret() > 0:

            ram_blks_iter = Blks(self._reg, self.apri).ram().ret()

            if self.startn is not None and self.length is None:

                for blk in ram_blks_iter:

                    try:
                        blk_len = len(blk)

                    except BlockNotOpenError as e:
                        raise BlockNotOpenError(RamBlkNotOpenErrMsg(self._reg, self.apri, self.startn).ret()) from e

                    else:

                        if blk.startn() == self.startn:
                            self._ret = (self.startn, blk_len)

            else:

                blk = next(ram_blks_iter)

                try:
                    blk_len = len(blk)

                except BlockNotOpenError as e:
                    raise BlockNotOpenError(RamBlkNotOpenErrMsg(self._reg, self.apri, self.startn).ret()) from e

                else:
                    self._ret = (blk.startn(), blk_len)

        return self

    def disk1(self, sro_txn):

        if self.startn is not None and self.length is None:
            prefix = DiskBlkPrefixesStartn(self._reg, self.apri, self.startn).disk1(sro_txn).ret()[0]

        else:
            prefix = DiskBlkPrefixes(self._reg, self.apri).disk1(sro_txn).ret()[0]

        for key, _ in sro_txn.prefix_iter(prefix):

            self._ret = StartnLength(self._reg, BLK_KEY_PREFIX_LEN, key).ret()
            break

        return self

    def disk2(self):
        return self

class DiskBlkFilenames(
    RegisterReadMethod, ProtectedRegisterMethod,
    arg_names = ('blk_key', 'compressed_key'),
    arg_types = (bytes, bytes)
):

    def __init__(self, reg, *args, **kwargs):

        super().__init__(reg, *args, **kwargs)
        self._compressed_val = self._blk_val = None

    def ram(self):
        return self

    def disk1(self, sro_txn):

        self._blk_val = sro_txn.get(self.blk_key)
        self._compressed_val = sro_txn.get(self.compressed_key)
        return self

    def disk2(self):

        blk_filename = self._reg._local_dir / bsdec(self._blk_val)

        if self._compressed_val != IS_NOT_COMPRESSED_VAL:

            compressed_filename = self._reg._local_dir / bsdec(self._compressed_val)

            if not compressed_filename.exists() or not blk_filename.exists():
                raise RegisterError("Compressed `Block` file or ghost file seems to be missing!")

            self._ret = (blk_filename, compressed_filename)

        else:

            if not blk_filename.exists():
                raise RegisterError("`Block` file seems to be missing!")

            self._ret = (blk_filename, None)

        return self


##################################################################
#                       WRITE METHODS                            #
##################################################################

class RegisterWriteMethod(
    RegisterDatabaseMethod, ABC,
    recurable = False,
    generator = False
):

    def type_value(self):

        super().type_value()
        self._reg._check_readwrite_raise(type(self).method_name)

class ExistingDiskBlockRegisterWriteMethod(RegisterWriteMethod, ABC):

    ret_blk_metadata = None
    ret_compressed_metadata = None

    def __init__(self, reg, *args, **kwargs):

        super().__init__(reg, *args, **kwargs)
        self._startn = self._length = self._blk_key = self._compressed_key = self._blk_filename = \
        self._compressed_filename = self._is_compressed = None
        self._errmsg = BlkNotFoundErrMsg(self._reg, self.apri, self.startn, self.length, None).ret()

        if not hasattr(self, 'ret_metadata'):
            self.ret_metadata = False

    def __init_subclass__(cls, **kwargs):

        super().__init_subclass__(**kwargs)
        cls.ret_blk_metadata = kwargs.pop('ret_blk_metadata')
        cls.ret_compressed_metadata = kwargs.pop('ret_compressed_metadata')

        if (
            'apri' not in cls.arg_names or 'startn' not in cls.arg_names or 'length' not in cls.arg_names or
            'ret_metadata' not in cls.arg_names
        ):
            raise ValueError

    def type_value(self):

        super().type_value()

        if self.startn is not None and self.startn < 0:
            raise ValueError("`startn` must be non-negative.")

        if self.length is not None and self.length < 0:
            raise ValueError("`length` must be non-negative.")

    def ram(self):
        return self

    def disk1(self, sro_txn):

        self._startn, self._length = ResolveStartnLength(self.apri, self.startn, self.length).disk1(sro_txn).ret()

        if self._startn is None:
            raise DataNotFoundError(self._errmsg) # see pattern VI.1

        self._blk_key, self._compressed_key = DiskBlkKeys(
            self._reg, self.apri, self._startn, self._length
        ).disk1(sro_txn).ret()

        if not disk_blk_keys_exist(self._blk_key, self._compressed_key, sro_txn):
            raise DataNotFoundError(self._errmsg)

        self._blk_filename, self._compressed_filename = DiskBlkFilenames(
            self._reg, self._blk_key, self._compressed_key
        ).disk1(sro_txn).ret()
        self._is_compressed = self._compressed_filename is not None
        return self

    def ret(self):

        if self.ret_metadata:

            if type(self).ret_blk_metadata and type(self).ret_compressed_metadata:
                return FileMetadata.from_path(self._blk_filename), FileMetadata.from_path(self._compressed_filename)

            elif type(self).ret_blk_metadata:
                return FileMetadata.from_path(self._blk_filename)

            elif type(self).ret_compressed_metadata:
                return FileMetadata.from_path(self._compressed_filename)

            else:
                return ValueError

        else:
            return None

class AddDiskBlk(
    RegisterWriteMethod,
    method_name = 'add_disk_blk',
    arg_names = ('blk', 'exists_ok', 'dups_ok', 'ret_metadata', 'timeout'),
    arg_types = (Block, bool, bool, bool, int),
    num_required_args = 1,
    default_args = (False, True, False, None)
):

    def __init__(self, reg, *args, **kwargs):

        self._add_apri = self._filename = self._blk_key = self._compressed_key = None
        super().__init__(reg, *args, **kwargs)

    def type_value(self):

        super().type_value()

        if len(self.blk) > self._reg._max_length:
            raise ValueError

        startn_head = self.blk.startn() // self._reg._startn_tail_mod

        if startn_head != self._reg.startn_head:
            raise IndexError(
                'The startn for the passed Block does not have the correct head:\n'
                f'tail_len      : {self._reg._startn_tail_length}\n'
                f'expected head : {self._reg._startn_head}\n'
                f'startn        : {self.blk.startn()}\n'
                f'startn head   : {startn_head}\n'
                'Please see the method set_startn_info to troubleshoot this error.'
            )

        return self

    def ram(self):
        return self

    def check(self, ro_txn):

        try:
            self.apri_json(ro_txn)

        except DataNotFoundError:
            self._add_apri = True

        else:

            apri_id_key = get_apri_id_key(self.apri_json(ro_txn))
            self._add_apri = not ro_txn.has_key(apri_id_key)

        self._filename = random_unique_filename(self._reg._local_dir, suffix = type(self._reg).file_suffix, length = 6)
        apri = self.blk.apri()
        startn = self.blk.startn()
        length = len(self.blk)

        if not self._add_apri:

            self._blk_key, self._compressed_key = self._reg._get_disk_blk_keys(
                apri, self.apri_json(ro_txn), False, startn, length, ro_txn
            )

            if not self.exists_ok and ro_txn.has_key(self._blk_key):
                raise DataExistsError(
                    f'Duplicate `Block` with the following data already exists in this `Register`: {apri}, startn = '
                    f'{startn}, length = {length}.'
                )

            if not self.dups_ok:

                prefix = self._reg._intervals_pre(apri, self.apri_json(ro_txn), False, ro_txn)
                int1 = (startn, length)

                for int2 in self._reg._intervals_disk(prefix, ro_txn):

                    if intervals_overlap(int1, int2):
                        raise DataExistsError(
                            'Attempted to add a `Block` with duplicate indices. Set `dups_ok` to `True` to suppress.'
                        )

        else:
            self._blk_key = self._compressed_key = None

        if length == 0:
            warnings.warn(f'Added a length 0 disk1 `Block`.\n{apri}\nstartn = {startn}\n{self._reg}')

        return self

    def disk1(self, sro_txn):

        self.check(sro_txn)
        apri = self.blk.apri()
        startn = self.blk.startn()
        length = len(self.blk)

        if self._add_apri:

            self._reg._add_apri_disk(apri, [], False, sro_txn)
            self._blk_key, self._compressed_key = self._reg._get_disk_blk_keys(
                apri, None, True, startn, length, sro_txn
            )

        filename_bytes = self._filename.name.json_encode_default('ASCII')
        sro_txn.put(self._blk_key, filename_bytes)
        sro_txn.put(self._compressed_key, IS_NOT_COMPRESSED_VAL)
        return self

    def disk2(self):

        if _debug == 1:
            raise KeyboardInterrupt

        type(self._reg).dump_disk_data(self.blk.segment(), self._filename, **self._kwargs)

        if _debug == 2:
            raise KeyboardInterrupt

        return self

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

class AppendDiskBlk(
    AddDiskBlk,
    method_name = 'append_disk_blk',
    arg_names = ('blk', 'ret_metadata', 'timeout'),
    arg_types = (Block, bool, int),
    num_required_args = 1,
    default_args = (False, None)
):

    def check(self, ro_txn):

        apri = self.blk.apri()
        startn = self.blk.startn()
        length = len(self.blk)

        try:
            self.apri_json(ro_txn)

        except DataNotFoundError:
            self._add_apri = True

        else:

            apri_id_key = get_apri_id_key(self.apri_json(ro_txn))
            self._add_apri = not ro_txn.has_key(apri_id_key)

        self._filename = random_unique_filename(self._reg._local_dir, suffix = type(self._reg).file_suffix, length = 6)

        if not self._add_apri:

            try:
                prefix = self._reg._maxn_pre(apri, self.apri_json(ro_txn), False, ro_txn)

            except DataNotFoundError: # if apri has no disk1 blks (used passed startn)
                pass

            else:
                startn = self._reg._maxn_disk(prefix, ro_txn) + 1

            self._blk_key, self._compressed_key = self._reg._get_disk_blk_keys(
                apri, self.apri_json(ro_txn), False, startn, length, ro_txn
            )

        else:
            self._blk_key = self._compressed_key = None

        if length == 0:
            warnings.warn(f'Added a length 0 disk1 `Block`.\n{apri}\nstartn = {startn}\n{self._reg}')

        return self

class RmvDiskBlk(
    ExistingDiskBlockRegisterWriteMethod,
    method_name = 'rmv_disk_blk',
    arg_names = ('apri', 'startn', 'length', 'missing_ok', 'timeout'),
    arg_types = (ApriInfo, int, int, bool, int),
    num_required_args = 1,
    default_args = (None, None, False, None),
    ret_blk_metadata = False,
    ret_compressed_metadata = False
):

    def disk1(self, sro_txn):

        super().disk1(sro_txn)
        sro_txn.delete(self._blk_key)
        sro_txn.delete(self._compressed_key)
        return self

    def disk2(self):

        if not is_deletable(self._blk_filename):
            raise OSError(f"Cannot delete Block file {str(self._blk_filename)}")

        if self._compressed_filename is not None and not is_deletable(self._compressed_filename):
            raise OSError(f"Cannot delete compressed Block file {str(self._compressed_filename)}")

        if _debug == 1:
            raise KeyboardInterrupt

        if self._compressed_filename is not None:

            if _debug == 2:
                raise KeyboardInterrupt

            self._blk_filename.unlink()

            if _debug == 3:
                raise KeyboardInterrupt

            self._compressed_filename.unlink()

            if _debug == 4:
                raise KeyboardInterrupt

        else:

            if _debug == 5:
                raise KeyboardInterrupt

            type(self._reg).clean_disk_data(self._blk_filename, **self._kwargs)

            if _debug == 6:
                raise KeyboardInterrupt

        if _debug == 7:
            raise KeyboardInterrupt

        return self

    def error(self, rrw_txn, rw_txn, e):

        if isinstance(e, RegisterRecoveryError):
            return e

        sorta_no_recover = RegisterRecoveryError(
            "Encountered an error after cleaning data files and deleting LMDB keys. Despite this error, the following "
            f"`Register` is in a state as if `rmv_disk_blk` did not encounter an error :\n{self._reg}"
        )
        sorta_no_recover.__cause__ = e

        try:

            if self._compressed_filename is not None:

                if self._compressed_filename.exists():

                    try:
                        self._blk_filename.touch()

                    except FileExistsError:
                        pass

                    rrw_txn.reverse(rw_txn)
                    return e

                else:
                    return sorta_no_recover

            elif not self._blk_filename.exists():
                return sorta_no_recover

            else:

                rrw_txn.reverse(rw_txn)
                return e

        except BaseException as ee:

            eee = RegisterRecoveryError(f"The following `Register` failed to recover from `rmv_disk_blk` :\n{self._reg}")
            eee.__cause__ = ee
            return eee

class Compress(
    ExistingDiskBlockRegisterWriteMethod,
    method_name = 'compress',
    arg_names = ('apri', 'startn', 'length', 'compression_level', 'ret_metadata', 'timeout'),
    arg_types = (ApriInfo, int, int, int, bool, int),
    num_required_args = 1,
    default_args = (None, None, 6, False, None),
    ret_blk_metadata = False,
    ret_compressed_metadata = True
):

    def type_value(self):

        super().type_value()

        if not (0 <= self.compression_level <= 9):
            raise ValueError("`compression_level` must be between 0 and 9, inclusive.")

        return self

    def disk1(self, sro_txn):

        super().disk1(sro_txn)

        if self._is_compressed:
            raise CompressionError(
                "The disk `Block` with the following data has already been compressed: " +
                f"{str(self.apri)}, startn = {self._startn}, length = {self._length}"
            )

        compressed_val = self._compressed_filename.name.json_encode_default("ASCII")
        sro_txn.put(self._compressed_key, compressed_val)
        return self

    def disk2(self):

        if _debug == 1:
            raise KeyboardInterrupt

        with zipfile.ZipFile(
                self._compressed_filename,  # target filename
                "x",  # zip mode (write, but don't overwrite)
                zipfile.ZIP_DEFLATED,  # compression mode
                True,  # use zip64
                self.compression_level
        ) as compressed_fh:

            if _debug == 2:
                raise KeyboardInterrupt

            compressed_fh.write(self._blk_filename, self._blk_filename.name)

            if _debug == 3:
                raise KeyboardInterrupt

        if _debug == 4:
            raise KeyboardInterrupt

        try:

            if _debug == 5:
                raise KeyboardInterrupt

            type(self._reg).clean_disk_data(self._blk_filename)

            if _debug == 6:
                raise KeyboardInterrupt

            self._blk_filename.touch()

            if _debug == 7:
                raise KeyboardInterrupt

        except BaseException as e:
            raise RegisterRecoveryError from e

        return self

    def error(self, rrw_txn, rw_txn, e):

        if isinstance(e, RegisterRecoveryError):
            return e

        if not self._blk_filename.exists():

            ee = RegisterRecoveryError(
                f"Deleted `Block` data file. The `Register` `{self._reg.shorthand()}` could not recover from failed "
                "`compress`."
            )
            ee.__cause__ = e
            return ee

        try:

            rrw_txn.reverse(rw_txn)

            try:
                self._compressed_filename.unlink()

            except FileNotFoundError:
                pass

        except BaseException as ee:

            eee = RegisterRecoveryError()
            eee.__cause__ = ee
            return eee

        else:
            return e

class Decompress(
    ExistingDiskBlockRegisterWriteMethod,
    method_name = 'decompress',
    arg_names = ('apri', 'startn', 'length', 'ret_metadata', 'timeout'),
    arg_types = (ApriInfo, int, int, bool, int),
    num_required_args = 1,
    default_args = (None, None, False, None),
    ret_blk_metadata = True,
    ret_compressed_metadata = False
):

    def __init__(self, reg, *args, **kwargs):

        self._temp_blk_filename = None
        super().__init__(reg, *args, **kwargs)

    def disk1(self, sro_txn):

        super().disk1(sro_txn)
        self._temp_blk_filename = self._blk_filename.parent / (self._blk_filename.stem + "_temp")

        if not self._is_compressed:
            raise DecompressionError(
                "The disk1 `Block` with the following data is not compressed: " +
                f"{str(self.apri)}, startn = {self._startn}, length = {self._length}"
            )

        sro_txn.put(self._compressed_key, IS_NOT_COMPRESSED_VAL)
        return self

    def disk2(self):

        if not is_deletable(self._blk_filename):
            raise OSError(f"Cannot delete ghost file `{str(self._blk_filename)}`.")

        if not is_deletable(self._compressed_filename):
            raise OSError(f"Cannot delete compressed file `{str(self._compressed_filename)}`.")

        if _debug == 1:
            raise KeyboardInterrupt

        self._temp_blk_filename.mkdir()

        with zipfile.ZipFile(self._compressed_filename, "r") as compressed_fh:

            if _debug == 2:
                raise KeyboardInterrupt

            compressed_fh.extract(self._blk_filename.name, self._temp_blk_filename)

            if _debug == 3:
                raise KeyboardInterrupt

        try:

            if _debug == 4:
                raise KeyboardInterrupt

            self._blk_filename.unlink()

            if _debug == 5:
                raise KeyboardInterrupt

            (self._temp_blk_filename / self._blk_filename.name).rename(self._blk_filename)

            if _debug == 6:
                raise KeyboardInterrupt

            self._compressed_filename.unlink()

            if _debug == 7:
                raise KeyboardInterrupt

            self._temp_blk_filename.rmdir()

            if _debug == 8:
                raise KeyboardInterrupt

        except BaseException as e:
            raise RegisterRecoveryError from e

        return self

    def error(self, rrw_txn, rw_txn, e):

        if isinstance(e, RegisterRecoveryError):
            return e

        try:

            try:
                shutil.rmtree(self._temp_blk_filename)

            except FileNotFoundError:
                pass

            rrw_txn.reverse(rw_txn)

        except BaseException as ee:

            eee = RegisterRecoveryError()
            eee.__cause__ = ee
            return ee

        else:
            return e

class ChangeApri(
    RegisterWriteMethod,
    method_name = 'change_apri',
    arg_names = ('old_apri', 'new_apri', 'diskonly'),
    arg_types = (ApriInfo, ApriInfo, bool),
    num_required_args = 2,
    default_args = (False,)
):

    def ram(self):

        if not self.diskonly:

            if not __Contains__(self._reg, self.old_apri).ram().ret():
                raise DataNotFoundError(self._reg._no_apri_err_msg(self.old_apri))

            if __Contains__(self._reg, self.new_apri).ram().ret():
                warnings.warn(f"This `Register` already has a reference to {self.new_apri}.")

            for apri in Apris(self._reg).ram().ret():

                if self.old_apri in apri:

                    apri_ = apri.change_info(self.old_apri, self.new_apri)

                    for blk in self._reg._ram_blks[apri]:
                        blk._apri = apri_

                    if len(self._reg._ram_blks[apri_]) == 0:
                        self._reg._ram_blks[apri_] = self._reg._ram_blks[apri]

                    else:
                        self._reg._ram_blks[apri_].extend(self._reg._ram_blks[apri])

                    del self._reg._ram_blks[apri]

    def pre(self, ro_txn):

        old_apri_json = self._reg._relational_encode_info(self.old_apri, ro_txn)

        try:
            new_apri_json = self._reg._relational_encode_info(self.new_apri, ro_txn)

        except DataNotFoundError:
            pass

        else:

            new_apri_id_key = __Contains__(self.new_apri).pre(ro_txn)

            if Register.___contains___disk(new_apri_id_key, r_txn):
                raise DataExistsError(f"This `Register` already has a reference to {new_apri}.")

        old_apri_id_key = get_apri_id_key(old_apri_json)
        old_id = self._get_apri_id(old_apri, old_apri_json, False, r_txn)
        old_id_apri_key = get_id_apri_key(old_id)
        return old_id, old_apri_id_key, old_id_apri_key

    def disk1(self, sro_txn):
        pass

    def disk2(self):
        pass

    def error(self, rrw_txn, rw_txn, e):
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

class __Contains__(
    RegisterReadMethod,
    method_name = '__contains__',
    arg_names = ('apri',),
    arg_types = (ApriInfo,),
    num_required_args = 1,
    default_args = (),
    recurable = True,
    generator = False
):

    def ram(self):

        self._ret = self._ret_after_ram = self.apri in self._reg._ram_blks.keys()
        return self

    def disk1(self, sro_txn):

        apri_id_key = get_apri_id_key(self.apri_json(sro_txn))
        self._ret = sro_txn.has_key(self._apri_id_key)
        return self

    def disk2(self):
        return self

class Apris(
    SortableRecurableRegisterReadMethod,
    method_name = 'apris',
    arg_names = ('keys', 'sort', 'diskonly', 'recursively'),
    arg_types = (check_iterable, bool, bool, bool),
    num_required_args = 0,
    default_args = ((), False, False, False),
    key = None
):

    def ram(self):

        if not self.diskonly:
            self._ret = itertools.chain(self._ret, self._reg._ram_blks.keys())

        return self

    def pre(self, ro_txn):
        return self

    def disk1(self, sro_txn):

        self._ret = itertools.chain(self._ret, (
            self._reg._relational_decode_info(ApriInfo, apri_json, sro_txn), apri_json
            for _, apri_json in sro_txn.prefix_iter(ID_APRI_KEY_PREFIX)
        ))
        return self

    def disk2(self):
        return self

    def ret(self):

        if len(self.args) == 0:
            yield from super().ret()

        else:

            for ret in super().ret():

                if (
                    len(type(ret)._reserved_kws) + len(self.keys) == len(ret.__dict__) and
                    all(hasattr(ret, key) for key in self.keys)
                ):
                    yield ret

class Intervals(
    SortableRecurableRegisterReadMethod,
    method_name = 'intervals',
    arg_names = ('apri', 'sort', 'combine', 'diskonly', 'recursively'),
    arg_types = (ApriInfo, bool, bool, bool, bool),
    num_required_args = 1,
    default_args = (False, False, False, False),
    key = interval_comparator
):

    def __init__(self, reg, *args, **kwargs):

        self._blk_prefix = None
        super().__init__(reg, *args, **kwargs)

    def ram(self):

        if not self.diskonly:
            self._ret = itertools.chain(self._ret, (
                (blk.startn(), len(blk)) for blk in Blks[self.apri]
            ))

        return self

    def pre(self, ro_txn):

        self._blk_prefix = self._reg._get_disk_blk_prefixes(self.apri, self.apri_json(ro_txn), False, ro_txn)[0]
        return self

    def disk1(self, sro_txn):

        self._ret = itertools.chain(self._ret, (
            self._reg._get_startn_length(BLK_KEY_PREFIX_LEN, key) for key, _ in sro_txn.prefix_iter(self._blk_prefix)
        ))
        return self

    def disk2(self):
        return self

    def ret(self):

        if self.combine:

            self.sort = True
            yield from combine_intervals(super().ret())

        else:
            yield from super().ret()

class Blk(
    RegisterReadMethod,
    method_name = 'blk',
    arg_names = ('apri', 'startn', 'length', 'decompress', 'diskonly', 'recursively', 'ret_metadata', 'timeout'),
    arg_types = (ApriInfo, int, int, bool, bool, bool, bool, int),
    num_required_args = 1,
    default_args = (None, None, False, False, False, False, None),
    recurable = True,
    generator = False
):

    def __init__(self, reg, *args, **kwargs):

        super().__init__(reg, *args, **kwargs)
        self._startn = self._length = self._blk_filename = self._compressed_filename = self._is_compressed = None
        self._resolve = ResolveStartnLength(self._reg, self.apri, self.startn, self.length)
        self._noblkerr = NoApriErrMsg(self._reg, self.apri).ret()

    def ram(self):

        if not __Contains__(self._reg, self.apri).ram().ret():
            raise DataNotFoundError(NoApriErrMsg(self._reg, self.apri))

        else:

            self._startn, self._length = self._resolve.ram().ret()

            if self._startn is None:
                raise DataNotFoundError(self._noblkerr)

            for blk in Blks(self._reg, self.apri).ram().ret():

                if blk.startn() == self._startn and len(blk) == self._length:

                    if not self.ret_metadata:
                        self._ret = blk

                    else:
                        self._ret = (blk, None)

                    self._ret_after_ram = True
                    return self

            raise DataNotFoundError(self._noblkerr)

    def pre(self, ro_txn):

        self._startn, self._length = self._resolve.pre(ro_txn).disk1(ro_txn).ret()
        blk_key, compressed_key = DiskBlkKeys(self._reg, self.apri, self.startn, self.length).pre(ro_txn).ret()

        if not disk_blk_keys_exist(blk_key, compressed_key, ro_txn):
            raise DataNotFoundError(self._noblkerr)

        self._blk_filename, self._compressed_filename = self._reg._get_disk_blk_filenames(
            blk_key, compressed_key, ro_txn
        )
        self._is_compressed = self._compressed_filename is not None

        if not self.decompress and self._is_compressed:
            raise CompressionError(
                'Could not load disk `Block` with the following data because the `Block` is compressed. '
                f'Please call either `{self._reg.shorthand()}.blk(..., decompress = True)` to temporarily decompress the '
                f'`Block`, or call `{self._reg.shorthand()}.decompress()` to permanently do so.\n'
                f'{self.apri}, startn = {self._startn}, length = {self._length}\n{self}'
            )

        return self

    def disk1(self, sro_txn):
        return self

    def disk2(self):

        if self._is_compressed:

            with tempfile.TemporaryDirectory() as temp_file:

                temp_file = Path(temp_file)

                with zipfile.ZipFile(self._compressed_filename, 'r') as compressed_fh:
                    compressed_fh.extract(self._blk_filename.name, temp_file)

                blk = Block(type(self._reg).load_disk_data(temp_file / self._blk_filename.name), self.apri, self._startn)

        else:

            seg = type(self._reg).load_disk_data(self._blk_filename, **self._kwargs)
            blk = Block(seg, self.apri, self._startn)

        if not self.ret_metadata:
            self._ret =blk

        elif self._is_compressed:
            self._ret = (blk, FileMetadata.from_path(self._compressed_filename))

        else:
            self._ret = (blk, FileMetadata.from_path(self._blk_filename))

class BlkByN(
    Blk,
    method_name = 'blk_by_n',
    arg_names = ('apri', 'n', 'decompress', 'diskonly', 'recursively', 'ret_metadata', 'timeout'),
    arg_types = (ApriInfo, int, bool, bool, bool, bool, int),
    num_required_args = 1,
    default_args = (None, None, False, False, False, False, None),
    recurable = True,
    generator = False
):
    def ram(self):

        for startn, length in Intervals(self._reg, self.apri, False, False, False, False).ram().ret():

            if startn <= self.n < startn + length:

                self.startn = startn
                self.length = length
                break

        else:
            raise DataNotFoundError(self._ret._blk_not_found_err_msg(True, False, False, self.apri, None, None, self.n))

        super().ram()
        return self

    def pre(self, ro_txn):

        for startn, length in Intervals(self._reg, self.apri, False, False, False, False).pre(ro_txn).ret():

            if startn <= self.n < startn + length:

                self.startn = startn
                self.length = length
                break

        else:
            raise DataNotFoundError(self._ret._blk_not_found_err_msg(False, True, False, self.apri, None, None, self.n))

        super().pre(ro_txn)
        return self


class Blks(
    SortableRecurableRegisterReadMethod,
    arg_names = ('apri', 'diskonly', 'recursively', 'ret_metadata', 'timeout'),
    arg_types = (ApriInfo, bool, bool, bool, int),
    num_required_args = 1,
    default_args = (False, False, False, None)
):

    def ram(self):
        pass

    def pre(self, ro_txn):
        pass

    def disk1(self, sro_txn):
        pass

    def disk2(self):
        pass


class ContainsInterval(
    RegisterReadMethod,
    arg_names = ('apri', 'startn', 'length', 'diskonly', 'recursively'),
    arg_types = (ApriInfo, int, int, bool, bool),
    num_required_args = 3,
    default_args = (False, False),
    recurable = True,
    generator = False
):

    def ram(self):

        for int__ in combine_intervals(self._intervals_ram(apri)):

            if intervals_subset(int_, int__):
                return True

        else:
            return False

    def pre(self, ro_txn):
        pass

    def disk1(self, sro_txn):
        pass

    def disk2(self):
        pass

class NumBlks(
    RegisterReadMethod,
    arg_names = ('apri', 'diskonly', 'recursively'),
    arg_types = (ApriInfo, bool, bool),
    num_required_args = 1,
    default_args = (False, False),
    recurable = True,
    generator = False
):

    def __init__(self, reg, *args, **kwargs):

        self._ret = 0
        super().__init__(reg, *args, **kwargs)

    def ram(self):

        if not self.diskonly:
            self._ret += len(self._reg._ram_blks[self.apri])

        return self

    def pre(self, ro_txn):
        return self

    def disk1(self, sro_txn):
        return self

    def disk2(self):
        return self