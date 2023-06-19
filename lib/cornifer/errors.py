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

class RegisterAlreadyOpenError(RegisterError):
    def __init__(self):
        super().__init__("This register is already opened.")

class CompressionError(RuntimeError):pass

class DecompressionError(RuntimeError):pass

class DataNotFoundError(RuntimeError):pass