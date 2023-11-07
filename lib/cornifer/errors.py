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

NOT_ABSOLUTE_ERROR_MESSAGE = (
    "The path `{0}` is not absolute."
)

class BlockNotOpenError(RuntimeError):pass

class RegisterError(RuntimeError):pass

class RegisterRecoveryError(RegisterError):pass

class RegisterNotOpenError(RegisterError):pass

class RegisterOpenError(RegisterError):pass

class RegisterAlreadyOpenError(RegisterError):

    def __init__(self, reg):
        super().__init__(
            f"The following `Register` is already opened in {'readonly' if reg._readonly else 'read-write'} mode :\n"
            f"{reg}"
        )

class CompressionError(RuntimeError):pass

class DecompressionError(RuntimeError):pass

class DataError(RuntimeError):pass

class DataNotFoundError(DataError):pass

class DataExistsError(DataError):pass

class CannotLoadError(RuntimeError):pass