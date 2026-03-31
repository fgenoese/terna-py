# -*- coding: utf-8 -*-

from setuptools import setup

# Read version from terna/terna.py
with open("terna/terna.py", encoding="utf-8") as f:
    for line in f:
        if line.startswith("__version__"):
            __version__ = line.split('"')[1]
            break

setup(
    version=__version__,
    long_description=open("README.md", encoding="utf-8").read(),
    long_description_content_type="text/markdown",
)