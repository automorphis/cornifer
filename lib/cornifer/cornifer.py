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

def cornify(directory, verbose = False, suppress_errors = True):

    directory = Path(directory)
    if not directory.is_dir():
        raise FileNotFoundError(f"The directory `{directory}` does not exist.")

    for d in directory.iterdir():
        register_dir = d / _REGISTER_DIR_NAME
        seqs_file =          register_dir / _SEQS_FILE_NAME
        sub_registers_file = register_dir / _SUB_REGISTERS_FILE_NAME
        cls_file =           register_dir / _CLS_FILE_NAME

        if (
            register_dir.is_dir()         and
            seqs_file.          is_file() and
            sub_registers_file. is_file() and
            cls_file.           is_file()
        ):

            with cls_file.open("r") as fh:
                for line in fh:
                    line = line.strip()
                    if len(line) > 0:
                        cls_str = line
                        break
                else:
                    e = Database_Error(
                        f"The file `{str(cls_file)}` must contain a class name."
                    )
                    log_raise_error(e,verbose,suppress_errors)
                    continue

            try:
                register = Register._from_local_dir(cls_str, directory)
            except ValueError:
                e = ValueError(
                    f"The file `{str(cls_file)}` contains an invalid class name."
                )
                log_raise_error(e, verbose, suppress_errors)
                continue

            register._set_local_dir(d)
            register._created = True

            if verbose:
                logging.info(f"Loaded register from `{str(register_dir)}`.")
