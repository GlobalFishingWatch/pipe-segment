#!/bin/bash

THIS_SCRIPT_DIR="$( cd "$(dirname "${BASH_SOURCE[0]}")" ; pwd -P )"

# import settings
source ${THIS_SCRIPT_DIR}/setup.cfg

# dataflow cannot process dependency_links included in setup.py
# so we have to download this package from github and pass it through
# with the extra_packages flag in parser.py

curl -sS $SEGMENTER_REMOTE_PACKAGE -o $SEGMENTER_LOCAL_PACKAGE

pip install -e .
pip install $SEGMENTER_LOCAL_PACKAGE

