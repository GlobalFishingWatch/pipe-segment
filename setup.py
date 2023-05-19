#!/usr/bin/env python

from setuptools import find_packages
from setuptools import setup

setup(
    name="pipe_segment",
    version='4.1.1',
    packages=find_packages(exclude=["test*.*", "tests"]),
    include_package_data=True
)
