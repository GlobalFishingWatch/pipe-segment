#!/usr/bin/env bash

PIPELINE='pipe_segment'
PIPELINE_VERSION=$(python -c "import pkg_resources; print(pkg_resources.get_distribution('${PIPELINE}').version)")

# add two spaces the the start of every line
# usage:
#   echo "indent this" | indent

indent() { sed 's/^/  /'; }