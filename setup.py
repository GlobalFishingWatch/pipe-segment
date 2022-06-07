#!/usr/bin/env python

from setuptools import find_packages
from setuptools import setup

setup(
    name="pipe_segment",
    version=__import__("pipe_segment").__version__,
    packages=find_packages(exclude=["test*.*", "tests"]),
)
