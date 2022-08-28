import platform
from datetime import datetime

from cornifer._utilities import BYTES_PER_MB, BYTES_PER_KB, LOCAL_TIMEZONE, BYTES_PER_GB


class FileMetadata:

    def __init__(self, created, modified, size):
        self.created = created
        self.modified = modified
        self.size = size
        self.size_GB = self.size // BYTES_PER_GB
        self.size_MB = (self.size % BYTES_PER_GB) // BYTES_PER_MB
        self.size_KB = (self.size % BYTES_PER_MB) // BYTES_PER_KB
        self.size_B  = self.size % BYTES_PER_KB

    @staticmethod
    def fromPath(path):

        stat = path.stat()

        if platform.system() == 'Windows':
            created = stat.st_ctime

        else:
            try:
                created = int(stat.st_birthtime)

            except AttributeError:
                created = None

        if created is not None:
            created = datetime.fromtimestamp(created, LOCAL_TIMEZONE)

        modified = int(stat.st_mtime)
        modified = datetime.fromtimestamp(modified, LOCAL_TIMEZONE)

        size = int(stat.st_size)

        return FileMetadata(created, modified, size)

    def __str__(self):
        smaller = False
        size_str = ""
        for symb, size in zip(["GB", "MB", "KB", "B"], [self.size_GB, self.size_MB, self.size_KB, self.size_B]):
            if smaller or size > 0 or symb == "B":
                size_str += f"{size} {symb} "
                smaller = True
        return (
            "Created: " + (self.created.strftime("%b %d %Y, %H:%M:%S") if self.created is not None else "unavailable") + ". " +
            "Modified: " + self.modified.strftime("%b %d %Y, %H:%M:%S") + ". " +
            f"Size: {size_str[:-1]}."
        )