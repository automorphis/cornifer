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

from setuptools import setup

setup(
    name = 'Cornifer',
    version = '0.1',
    description = "An easy-to-use data manager for experimental mathematics.",
    long_description = "An easy-to-use data manager for experimental mathematics.",

    author = "Michael P. Lane",
    author_email = "mlanetheta@gmail.com",
    url = "https://github.com/automorphis/cornifer",

    package_dir = {"": "lib"},

    packages = [
        "cornifer",
        "cornifer.utilities"
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