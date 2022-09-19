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

from cornifer.errors import NOT_ABSOLUTE_ERROR_MESSAGE
from cornifer._utilities import BASE52

REG_FILENAME           = "register"
VERSION_FILEPATH       = f"{REG_FILENAME}/version.txt"
MSG_FILEPATH           = f"{REG_FILENAME}/message.txt"
CLS_FILEPATH           = f"{REG_FILENAME}/class.txt"
DATABASE_FILEPATH      = f"{REG_FILENAME}/database"
MAP_SIZE_FILEPATH      = f"{REG_FILENAME}/mapsize.txt"

LOCAL_DIR_CHARS        = BASE52
COMPRESSED_FILE_SUFFIX = ".zip"


def checkRegStructure(localDir):
    """
    :param localDir: (type `pathlib.Path`) Absolute.
    :raise FileNotFoundError
    """

    if not localDir.is_absolute():
        raise ValueError(NOT_ABSOLUTE_ERROR_MESSAGE.format(str(localDir)))

    problems = []

    if not localDir.is_dir():
        problems.append(str(localDir))

    for path in [VERSION_FILEPATH, MSG_FILEPATH, CLS_FILEPATH, MAP_SIZE_FILEPATH]:
        if not (localDir / path).is_file():
            problems.append(str(localDir / path))

    for path in [DATABASE_FILEPATH]:
        if not (localDir / path).is_dir():
            problems.append(str(localDir / path))

    if len(problems) > 0:
        raise FileNotFoundError(
            "Could not find the following files or directories: " +
            ", ".join(problems)
        )
