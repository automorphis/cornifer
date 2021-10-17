"""
    Cornifer, an intuitive data manager for empirical mathematics
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

import inspect
import json
from itertools import chain
from pathlib import Path
from abc import ABC, abstractmethod

from cornifer.errors import Sub_Register_Cycle_Error, Database_Error, Data_Not_Found_Error
from cornifer.sequences import Sequence_Description
from cornifer.utilities import intervals_overlap, random_unique_filename, safe_overwrite_file, open_leveldb, \
    leveldb_has_key

_REGISTER_LEVELDB_NAME = "register"

class Register(ABC):

    ___GETITEM___ERROR_MSG = (
"""
Acceptable syntax is, for example:
   register[descr, 5]
   register[descr, 10:20]
   register[descr, 10:20, True]
   register[descr, 10:20:3, True]
   register[descr, 10:20:-3, True]
where `descr` is an instance of `Sequence_Description`. The optional third parameter tells 
the register whether to search recursively for the requested data; the default value, 
`False`, means that the register will not. Negative indices are not permitted, so you 
cannot do the following:
   register[descr, -5]
   register[descr, -5:-10:-1]
"""
    )

    #################################
    #        INITIALIZATION         #

    def __init__(self, saves_directory, msg = ""):
        self.saves_directory = Path(saves_directory)

        if not self.saves_directory.is_file():
            raise FileNotFoundError(
                f"You must create the file `{str(self.saves_directory)}` before calling "+
                f"`{type(self)}({str(self.saves_directory)})`."
            )

        if not isinstance(msg, str):
            raise TypeError(f"The `msg` argument must be a `str`. Passed type of `msg`: `{str(type(msg))}`.")

        self._msg = msg

        self._local_dir = None
        self._register_file = None

        self._register_bytes = None
        self._register_cls_bytes = type(self).__name__.encode()
        self._str_bytes = str(self).encode()

        self._ram_seqs = {}
        self._disk_seqs = {}
        self._sub_registers = []

        self._loaded = False
        self._created = False

    _constructors = {}

    @staticmethod
    def _from_name(name, saves_directory):
        if name == "Register":
            raise ValueError(
                "`Register` is an abstract class, meaning that `Register` itself cannot be instantiated, " +
                "only its concrete subclasses."
            )
        try:
            return Register._constructors[name](saves_directory)
        except KeyError:
            raise ValueError(
                f"`Register` is not aware of a subclass called \"{name}\". Please add the subclass to " +
                f"`Register` via `Register.add_subclass({name})`."
            )

    @staticmethod
    def add_subclass(subclass):
        if not inspect.isclass(subclass):
            raise TypeError("The `subclass` argument must be a class.")
        if not issubclass(subclass, Register):
            raise TypeError(f"The class `{subclass.__name__}` must be a subclass of `Register`.")
        Register._constructors[subclass.__name__] = subclass

    #################################
    #    PUBLIC REGISTER METHODS    #

    def __eq__(self, other):
        return self._local_dir == other._local_dir

    def __hash__(self):
        raise TypeError("`Register` is not a hashable type.")

    def __str__(self):
        return self._msg

    def set_msg(self, msg):
        self._msg = msg
        self._str_bytes = str(self).encode()
        if self._created:
            with open_leveldb(self._register_file) as db:
                db.put(b"str", self._str_bytes)

    #################################
    #    PROTEC REGISTER METHODS    #

    def _load(self, recursively = False):

        if not self._created:
            return

        if self._loaded:
            if recursively:
                for sub_register in self._sub_registers:
                    sub_register._load(True)
            return

        if not self.saves_directory.is_dir():
            raise FileNotFoundError(f"The path `{self.saves_directory}` must be an existing directory.")

        if not self._local_dir.is_dir():
            raise FileNotFoundError(
                f"The directory `{self.saves_directory}` must contain the subdirectory " +
                f"`{self._local_dir.name}`."
            )

        if not self._disk_seqs_file.is_file() or not self._sub_registers_file.is_file():
            raise FileNotFoundError(
                f"The directory `{self._register_dir}` must contain three files named " +
                f"`{_SEQS_FILE_NAME}` and `{_SUB_REGISTERS_FILE_NAME}` and " +
                f"`{_CLS_FILE_NAME}`."
            )

        for descr, start_n, length, filename in self._iter_seqs_file():
            self._disk_seqs[(descr, start_n, length)] = filename

        for cls_str, filename in self._iter_leveldb_sub_registers():
            register = Register._from_name(cls_str, filename)
            self.add_sub_register(register)

        self._loaded = True
        self._created = True

        if recursively:
            for sub_register in self._sub_registers:
                sub_register._load(True)

    def _create(self):
        if not self._created:
            self._created = True
            self._set_local_dir(random_unique_filename(self.saves_directory))
            Path.mkdir(self._local_dir)
            with open_leveldb(self._register_file, True) as db:
                with db.write_batch(transaction = True) as wb:
                    wb.put(b"cls", self._register_bytes)
                    wb.put(b"str", self._str_bytes)

    def _set_local_dir(self, filename):
        self._local_dir = filename
        self._register_file = self._local_dir / _REGISTER_LEVELDB_NAME
        self._register_bytes = (self._local_dir + "-sub").encode()

    #################################
    #     PUBLIC DESCR METHODS      #

    def get_descrs(self, recursively = False):
        self._load()
        descrs = set(descr for descr,_ in self._disk_seqs.keys())
        if recursively:
            for sub_register in self._sub_registers:
                descrs.update(sub_register.get_descrs(True))
        return descrs

    #################################
    #     PROTEC DESCR METHODS      #

    #################################
    #  PUBLIC SUB-REGISTER METHODS  #

    def add_sub_register(self, register):
        self._create()
        self._load()
        if register not in self._sub_registers:
            if register._check_no_cycles(self):
                self._sub_registers.append(register)
                register._create()
                with open_leveldb(self._register_file) as db:
                    db.put(register._register_bytes, register._register_cls_bytes)
            else:
                raise Sub_Register_Cycle_Error(self, register)

    def remove_sub_register(self, register):

        with open_leveldb(self._register_file) as db:
            changed = leveldb_has_key(db, self._register_bytes)
            db.remove(self._register_bytes)

        if changed:
            self._load()
            self._sub_registers.remove(register)

    #################################
    #  PROTEC SUB-REGISTER METHODS  #

    def _check_no_cycles(self, original):
        self._load()
        if any(original == sub_register for sub_register in self._sub_registers):
            return False
        if all(sub_register._check_no_cycles(original) for sub_register in self._sub_registers):
            return True

    #################################
    #    PUBLIC DISK SEQ METHODS    #

    @classmethod
    @abstractmethod
    def dump_disk_seq(cls, seq, filename):
        """Dump a `Sequence` to disk.

        This method should not change any properties of any `Register`, which is why it is a classmethod and
        not an instancemethod. It merely takes the data wrapped by `seq` and dumps it to the disk.

        Most use-cases prefer the instancemethod `add_disk_seq`.

        :param seq: (type `Sequence`)
        :param filename: (type `pathlib.Path`) The filename of the dumped sequence. You may edit this filename
        if necessary (such as by adding a suffix), but you must return the edited filename.
        :return: (type `pathlib.Path) The actual filename of the sequence on the disk.
        """

    @classmethod
    @abstractmethod
    def load_disk_seq(cls, filename):
        """Load raw data from the disk.

        This method should not change any properties of any `Register`, which is why it is a classmethod and
        not an instancemethod. It merely loads the data saved on the disk and returns it.

        Most use-cases prefer the method `get_seq` (which also returns RAM sequences).

        :param filename: (type `pathlib.Path`) Where to load the sequence from. You may need to edit this
        filename if necessary, such as by adding a suffix. You do not need to return the edited filename.
        :return: (any type) The data loaded from the disk.
        """

    def add_disk_seq(self, seq):
        """TODO

        :param seq:
        :return:
        """

        descr = seq.get_descr()
        start_n = seq.start_n
        length = len(seq)

        self._create()
        self._load()
        if (descr, start_n, length) not in self._disk_seqs.keys():

            filename = random_unique_filename(self._local_dir)
            filename = type(self).dump_disk_seq(seq, filename)
            filename_bytes = filename.encode()

            with open_leveldb(self._register_file) as db:
                with db.write_batch(transaction = True) as wb:
                    wb.put(filename_bytes + b"-descr", descr.to_json.encode())
                    wb.put(filename_bytes + b"-start_n", str(start_n).encode())
                    wb.put(filename_bytes + b"-length", str(length).encode())

            #TODO delete data if leveldb write fails

            self._disk_seqs[(descr, start_n, length)] = filename

    def remove_disk_seq(self, descr, start_n, length):
        new_content = ""
        changed = False
        for _descr, _start_n, _length, _filename in self._iter_seqs_file():
            if not(descr == _descr and start_n == _start_n and _length == length):
                new_content += _descr.to_json() + "\n" + str(_start_n) + "\n" + str(_length) + "\n" + str(_filename) + "\n"
            else:
                changed = True
        if changed:
            safe_overwrite_file(self._disk_seqs_file, new_content)
            self._load()
            del self._disk_seqs[(descr, start_n, length)]

    #################################
    #    PROTEC DISK SEQ METHODS    #

    #################################
    #    PUBLIC RAM SEQ METHODS     #

    def add_ram_seq(self, seq):

        descr = seq.get_descr()
        start_n = seq.get_start_n()
        if (descr,start_n) not in self._ram_seqs.keys():
            self._ram_seqs[(descr, start_n)] = seq

    def remove_ram_seq(self, seq):

        try:
            del self._ram_seqs[(seq.get_descr(), seq.get_start_n())]
        except KeyError:
            pass

    #################################
    #    PROTEC RAM SEQ METHODS     #

    #################################
    # PUBLIC RAM & DISK SEQ METHODS #

    def get_seq(self, descr, n, recursively = True):

        for _ram_seq in self._ram_seqs:
            if _ram_seq.get_descr() == descr and n in _ram_seq:
                return _ram_seq

        self._load()
        for (_descr,_start_n,_length), _filename in self._disk_seqs.items():
            if _descr == descr and _start_n <= n < _start_n + _length:
                return type(self).load_disk_seq(_filename)

        if recursively:
            for _sub_register in self._sub_registers:
                try:
                    return _sub_register.get_seq(descr, n, True)
                except Data_Not_Found_Error:
                    pass

        raise Data_Not_Found_Error

    def __getitem__(self, descr_and_n_and_recursively):
        short = descr_and_n_and_recursively
        if (
            isinstance(short, tuple) or
            not(2 <= len(short) <= 3) or
            not(isinstance(short[0], Sequence_Description)) or
            not(isinstance(short[1], (int,slice))) or
            (len(short) == 3 and not isinstance(short[2],bool))
        ):
            raise TypeError(Register.___GETITEM___ERROR_MSG)

        if (
            isinstance(short[1], slice) and (
                (short.start is not None and short.start < 0) or
                (short.stop  is not None and short.stop  < 0)
            )
        ):
            raise ValueError(Register.___GETITEM___ERROR_MSG)

        if len(short) == 2:
            descr, n = descr_and_n_and_recursively
            recursively = False
        else:
            descr, n, recursively = descr_and_n_and_recursively

        if isinstance(n, slice):
            return _Seq_Iter(self, descr, n, recursively)
        else:
            return self.get_seq(descr, n, recursively)[n]

    def list_seqs_calculated(self, recursively = False):

        ret = {}
        for descr in self.get_descrs(recursively):
            intervals_sorted = sorted(
                [
                    (start_n, length)
                    for _descr, start_n, length in self._get_ram_and_disk_metadatas(recursively)
                    if descr == _descr
                ],
                key = lambda t: t[0]
            )
            intervals_reduced = []
            for int1 in intervals_sorted:
                for i, int2 in enumerate(intervals_reduced):
                    if intervals_overlap(int1,int2):
                        a1, l1 = int1
                        a2, l2 = int2
                        if a2 + l2 < a1 + l1:
                            intervals_reduced[i] = (a2, a1 + l1 - a2)
                            break
                else:
                    intervals_reduced.append(int1)
            ret[descr] = intervals_reduced
        return ret

    #################################
    # PROTEC RAM & DISK SEQ METHODS #

    def _get_ram_and_disk_metadatas(self, recursively = False):
        self._load()
        metadatas = set()
        if recursively:
            for register in self._sub_registers:
                metadatas.update(register._get_ram_and_disk_metadatas(True))
        metadatas.update(chain(self._ram_seqs.keys(), self._disk_seqs.keys()))
        return metadatas

class Pickle_Register(Register):

    @classmethod
    def dump_disk_seq(cls, seq, filename):
        pass

    @classmethod
    def load_disk_seq(cls, filename):
        pass

Register.add_subclass(Pickle_Register)

class NumPy_Register(Register):

    @classmethod
    def dump_disk_seq(cls, seq, filename):
        pass

    @classmethod
    def load_disk_seq(cls, filename):
        pass

Register.add_subclass(NumPy_Register)

class _Seq_Iter:

    def __init__(self, register, descr, slc, recursively = False):
        self.register = register
        self.descr = descr
        self.slc = slice(
            slc.start if slc.start else 0,
            slc.stop,
            slc.step  if slc.step  else 1
         )
        self.recursively = recursively
        self.curr_n = slc.start if slc.start else 0
        self.curr_seq = None

    def __iter__(self):
        return self

    def __next__(self):
        if self.slc.stop is not None and self.curr_n >= self.slc.stop:
            raise StopIteration
        if not self.curr_seq or self.curr_n not in self.curr_seq:
            self.curr_seq = self.register.get_seq(self.descr,self.curr_n,self.recursively)
        ret = self.curr_seq[self.curr_n]
        self.curr_n += self.slc.step
        return ret