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
from pathlib import Path

from cornifer.errors import NOT_ABSOLUTE_ERROR_MESSAGE
from cornifer._utilities import BASE52

REG_FILENAME           = Path("register")
VERSION_FILEPATH       = REG_FILENAME / "version.txt"
SHORTHAND_FILEPATH     = REG_FILENAME / "shorthand.txt"
MSG_FILEPATH           = REG_FILENAME / "message.txt"
CLS_FILEPATH           = REG_FILENAME / "class.txt"
DATABASE_FILEPATH      = REG_FILENAME / "database"
MAP_SIZE_FILEPATH      = REG_FILENAME / "mapsize.txt"
TMP_DIR_FILEPATH       = REG_FILENAME / "tmpdir.txt"
WRITE_DB_FILEPATH     = REG_FILENAME / "writedb.txt"

LOCAL_DIR_CHARS        = BASE52
COMPRESSED_FILE_SUFFIX = ".zip"


def check_reg_structure(local_dir):
    """
    :param local_dir: (type `pathlib.Path`) Absolute.
    :raise FileNotFoundError
    """

    if not local_dir.is_absolute():
        raise ValueError(NOT_ABSOLUTE_ERROR_MESSAGE.format(str(local_dir)))

    problems = []

    if not local_dir.is_dir():
        problems.append(str(local_dir))

    for path in [VERSION_FILEPATH, MSG_FILEPATH, CLS_FILEPATH, MAP_SIZE_FILEPATH, TMP_DIR_FILEPATH, WRITE_DB_FILEPATH]:
        if not (local_dir / path).is_file():
            problems.append(str(local_dir / path))

    for path in [DATABASE_FILEPATH]:
        if not (local_dir / path).is_dir():
            problems.append(str(local_dir / path))

    if len(problems) > 0:
        raise FileNotFoundError(
            "Could not find the following files or directories: "
            ", ".join(problems)
        )
