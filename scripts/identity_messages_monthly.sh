#!/bin/bash

THIS_SCRIPT_DIR="$( cd "$(dirname "${BASH_SOURCE[0]}")" ; pwd -P )"

ASSETS=${THIS_SCRIPT_DIR}/../assets


display_usage() {
	echo -e "\nUsage:\nidentity_messages_monthly.sh SOURCE DEST START_DATE END_DATE\n"
	}


if [[ $# -le 3 || $# -gt 4 ]]
then
    display_usage
    exit 1
fi

SOURCE=$1
DEST=$2
START_DATE=$3
END_DATE=$4


QUERY=${ASSETS}/identity_messages_monthly.sql.j2
SQL=$(jinja2 ${QUERY} -D source=${SOURCE} -D start_date=${START_DATE} -D end_date=${END_DATE})

echo "${SQL}" | bq query --max_rows=0 --allow_large_results --replace \
          --use_legacy_sql --destination_table ${DEST}






