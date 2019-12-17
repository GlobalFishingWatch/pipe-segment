#!/usr/bin/env python

"""
Setup script for pipe-segement
"""

import codecs
import os

from setuptools import find_packages
from setuptools import setup
from pipe_tools.beam.requirements import requirements as DATAFLOW_PINNED_DEPENDENCIES

package = __import__('pipe_segment')

DEPENDENCIES = [
    "pytest",
    "nose",
    "ujson",
    "pandas",
    "pytz",
    "udatetime",
    "newlinejson",
    "python-stdnum",
    "gpsdio-segment", #==0.19-dev",
    "pipe-tools==3.0.0",
    "shipdataprocess==0.6.9",
    "jinja2-cli",
    "google-apitools"
]



with codecs.open('README.md', encoding='utf-8') as f:
    readme = f.read().strip()

setup(
    author=package.__author__,
    author_email=package.__email__,
    description=package.__doc__.strip(),
    include_package_data=True,
    install_requires=DEPENDENCIES + DATAFLOW_PINNED_DEPENDENCIES,
    license="Apache 2.0",
    long_description=readme,
    name='pipe-segment',
    packages=find_packages(exclude=['test*.*', 'tests']),
    url=package.__source__,
    version=package.__version__,
    zip_safe=True,
)
