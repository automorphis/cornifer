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

class Register_Error(RuntimeError):pass

class Register_Already_Open_Error(Register_Error):
    def __init__(self):
        super().__init__("This register is already opened.")

class Compression_Error(RuntimeError):pass

class Decompression_Error(RuntimeError):pass

class Data_Not_Found_Error(RuntimeError):pass