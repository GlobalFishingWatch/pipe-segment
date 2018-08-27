#!/usr/bin/env bash
# run fetch_messages.sql.j2

set -e

source pipe-tools-utils

THIS_SCRIPT_DIR="$( cd "$(dirname "${BASH_SOURCE[0]}")" ; pwd -P )"


PROCESS_DATE=$1
SOURCE=$2
DEST=$3

YYYYMMDD=$(yyyymmdd ${PROCESS_DATE})
SOURCE=${SOURCE//:/.}${YYYYMMDD}
DEST=${DEST}${YYYYMMDD}

SQL=${THIS_SCRIPT_DIR}/fetch_messages.sql.j2
QUERY=$(jinja2 ${SQL} -D source=${SOURCE})

echo "${QUERY}" | \
  bq query --headless --max_rows=0 --allow_large_results --replace \
  --destination_table ${DEST}

bq update  --description "${QUERY}" ${DEST}
