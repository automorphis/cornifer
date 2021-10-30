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

class Database_Error(OSError):
    def __init__(self, leveldb_file, msg):
        super().__init__(
            "Failed to access the following `Register` database:\n" +
            f"{str(leveldb_file)}\n" +
            str(msg)
        )

class Critical_Database_Error(Database_Error):
    def __init__(self, leveldb_file, msg):
        super().__init__(
            leveldb_file,
            msg + "\nThis is a critical error. The `Register` database has been corrupted."
        )

class Register_Error(RuntimeError):pass

class Register_Already_Closed_error(Register_Error):
    def __init__(self):
        super().__init__("This register is already closed.")

class Register_Already_Open_Error(Register_Error):
    def __init__(self):
        super().__init__("This register is already opened.")

class Register_Not_Open_Error(Register_Error):
    def __init__(self, method_name):
        super().__init__(
            f"You must open this register via `with reg.open() as reg:` before calling the method " +
            f"`reg.{method_name}()`."
        )

class Register_Not_Created_Error(Register_Error):
    def __init__(self, methodname):
        super().__init__(
            f"The `Register` database has not been created. You must do `with reg.open() as reg:` at " +
            f"least once before calling the method `{methodname}`."
        )

class Data_Not_Dumped_Error(OSError):pass

class Data_Not_Loaded_Error(OSError):pass

class Data_Not_Found_Error(OSError):pass

class Keyword_Argument_Error(RuntimeError):pass

class Subregister_Cycle_Error(Register_Error):
    def __init__(self, parent, child):
        super().__init__(
            "Attempting to add this register as a sub-register will created a directed cycle in the " +
            "subregister relation. \n" +
            f"Description of the intended super-register:\n\"{str(parent)}\"\n" +
            f"Description of the intended sub-register:\n\"{str(child)}\""
        )