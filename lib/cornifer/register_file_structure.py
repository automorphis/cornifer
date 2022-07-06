from cornifer.errors import NOT_ABSOLUTE_ERROR_MESSAGE
from cornifer.utilities import BASE54

REGISTER_FILENAME           = "register"
VERSION_FILEPATH            = f"{REGISTER_FILENAME}/version.txt"
MSG_FILEPATH                = f"{REGISTER_FILENAME}/message.txt"
CLS_FILEPATH                = f"{REGISTER_FILENAME}/class.txt"
DATABASE_FILEPATH           = f"{REGISTER_FILENAME}/database"

LOCAL_DIR_CHARS             = BASE54
COMPRESSED_FILE_SUFFIX      = ".zip"


def check_register_structure(local_dir):
    """
    :param local_dir: (type `pathlib.Path`) Absolute.
    :raise FileNotFoundError
    """

    if not local_dir.is_absolute():
        raise ValueError(NOT_ABSOLUTE_ERROR_MESSAGE.format(str(local_dir)))

    problems = []

    if not local_dir.is_dir():
        problems.append(str(local_dir))

    for path in [VERSION_FILEPATH, MSG_FILEPATH, CLS_FILEPATH]:
        if not (local_dir / path).is_file():
            problems.append(str(local_dir / path))

    for path in [DATABASE_FILEPATH]:
        if not (local_dir / path).is_dir():
            problems.append(str(local_dir / path))

    if len(problems) > 0:
        raise FileNotFoundError(
            "Could not find the following files or directories: " +
            ", ".join(problems)
        )
