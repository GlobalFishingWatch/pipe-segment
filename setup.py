#!/usr/bin/env python

"""
Setup script for pipe-segement
"""

from setuptools import find_packages
from setuptools import setup

import codecs

package = __import__("pipe_segment")

DEPENDENCIES = [
    "pytest",
    "nose",
    "ujson",
    "pandas",
    "pytz",
    "udatetime",
    "newlinejson",
    "python-stdnum",
    "gpsdio-segment",
    "jinja2-cli",
    "google-apitools",
    "numpy<1.20.0",
    "apache-beam[gcp]==2.32.0"
    # "apache-beam[gcp]==2.32.0",
]


with codecs.open("README.md", encoding="utf-8") as f:
    readme = f.read().strip()

setup(
    author=package.__author__,
    author_email=package.__email__,
    description=package.__doc__.strip(),
    include_package_data=True,
    install_requires=DEPENDENCIES,
    license="Apache 2.0",
    long_description=readme,
    name="pipe-segment",
    packages=find_packages(exclude=["test*.*", "tests"]),
    url=package.__source__,
    version=package.__version__,
    zip_safe=True,
)
