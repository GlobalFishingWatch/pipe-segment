#!/usr/bin/env python

import setuptools

setuptools.setup(
    name="pipe_segment",
    version='4.3.0',
    author="Global Fishing Watch.",
    # author_email="",
    # maintainer="",
    description=(
        "Divides vessel tracks into contiguous 'segments' "
        "separating out signals that come from two or more vessels "
        "which are broadcasting using the same MMSI at the same time."
    ),
    long_description_content_type="text/markdown",
    url="https://github.com/GlobalFishingWatch/pipe-segment",
    packages=setuptools.find_packages(exclude=["test*.*", "tests"]),
    include_package_data=True,
    classifiers=[
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Programming Language :: Python :: 3.12",
        "License :: OSI Approved :: GNU General Public License v3 (GPLv3)",
        "Operating System :: OS Independent",
    ],
    install_requires=[
        'apache-beam[gcp]<3',
        'backports.zoneinfo<1;python_version<"3.9"',
        'gpsdio-segment '
        '@ https://codeload.github.com/GlobalFishingWatch/gpsdio-segment/tar.gz/v3.0.0',
        'jinja2<4',
        'jinja2-cli<1',
        'newlinejson<2',
        'python-stdnum<2',
        'shipdataprocess<1',
        'ujson<6',
    ]
)
