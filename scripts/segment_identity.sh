#!/bin/bash

THIS_SCRIPT_DIR="$( cd "$(dirname "${BASH_SOURCE[0]}")" ; pwd -P )"

ASSETS=${THIS_SCRIPT_DIR}/../assets


display_usage() {
	echo -e "\nUsage:\nsegment_identity.sh SOURCE DEST \n"
	}


if [[ $# -le 1 || $# -gt 2 ]]
then
    display_usage
    exit 1
fi

SOURCE=$1
DEST=$2


QUERY=${ASSETS}/segment_identity.sql.j2
SQL=$(jinja2 ${QUERY} -D source=${SOURCE})

echo "${SQL}" | bq query --max_rows=0 --allow_large_results --replace \
          --use_legacy_sql --destination_table ${DEST}






