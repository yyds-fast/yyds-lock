#!/usr/bin/env python
# -*- coding:utf-8 -*-

from setuptools import setup, find_packages
from codecs import open
import os

about = {}
here = os.path.abspath(os.path.dirname(__file__))
with open(os.path.join(here, "yyds_lock", "__version__.py"), "r", "utf-8") as f:
    exec(f.read(), about)

try:
    with open(os.path.join(here, "README.md"), "r", encoding="utf-8") as fh:
        long_description = fh.read()
except FileNotFoundError:
    long_description = about["__description__"]

setup(
    name=about["__title__"],
    version=about["__version__"],
    author=about["__author__"],
    author_email=about["__author_email__"],
    description=about["__description__"],
    long_description=long_description,
    long_description_content_type="text/markdown",
    url=about["__url__"],
    license=about.get("__license__", "MIT"),
    license_files=["LICENSE"],
    packages=find_packages(),
    include_package_data=True,
    python_requires='>=3.7',
    classifiers=[
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Programming Language :: Python :: 3.12",
        "Operating System :: OS Independent",
    ],
    install_requires=[],
)
