from cornifer.errors import NOT_ABSOLUTE_ERROR_MESSAGE
from cornifer._utilities import BASE54

REG_FILENAME           = "register"
VERSION_FILEPATH       = f"{REG_FILENAME}/version.txt"
MSG_FILEPATH           = f"{REG_FILENAME}/message.txt"
CLS_FILEPATH           = f"{REG_FILENAME}/class.txt"
DATABASE_FILEPATH      = f"{REG_FILENAME}/database"

LOCAL_DIR_CHARS        = BASE54
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

    for path in [VERSION_FILEPATH, MSG_FILEPATH, CLS_FILEPATH]:
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
