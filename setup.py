"""
    Beta Expansions of Salem Numbers, calculating periods thereof
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

from setuptools import setup

# get version
with (Path(__file__).parent / "lib" / "cornifer" / "version.py").open("r") as fh:
    # hacky way to import `cornifer.version.CURRENT_VERSION`
    exec(fh.read())

setup(
    name = 'cornifer',
    version = CURRENT_VERSION,
    description = "An easy-to-use data manager for experimental mathematics.",
    long_description = "An easy-to-use data manager for experimental mathematics.",
    long_description_content_type="text/plain",

    author = "Michael P. Lane",
    author_email = "mlanetheta@gmail.com",
    url = "https://github.com/automorphis/cornifer",

    package_dir = {"": "lib"},

    packages = [
        "cornifer",
        "cornifer._utilities"
    ],

    install_requires = [
        'oldest-supported-numpy',
        'lmdb>=1.2.1'
    ],

    classifiers=[
        "Programming Language :: Python",
        "Programming Language :: Python :: Implementation :: CPython",
        "Programming Language :: Python :: 3.10",
        "Topic :: Database",
        "Topic :: Database :: Front-Ends",
        "Topic :: Scientific/Engineering :: Mathematics"
    ],

    test_suite = "tests",

    zip_safe=False
)