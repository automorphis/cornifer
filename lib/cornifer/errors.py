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

class Data_Not_Found_Error(RuntimeError):pass

class Database_Error(RuntimeError):pass

class Sub_Register_Cycle_Error(RuntimeError):
    def __init__(self, parent, child):
        super().__init__(
            "Attempting to add this register as a sub-register will created a directed cycle in the " +
            "subregister relation. \n" +
            f"Description of the intended super-register:\n\"{str(parent)}\"\n" +
            f"Description of the intended sub-register: \n\"{str(child)}\""
        )