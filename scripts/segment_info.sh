#!/bin/bash
set -e

source pipe-tools-utils

THIS_SCRIPT_DIR="$( cd "$(dirname "${BASH_SOURCE[0]}")" ; pwd -P )"

ASSETS=${THIS_SCRIPT_DIR}/../assets
source ${THIS_SCRIPT_DIR}/pipeline.sh

display_usage() {
	echo -e "\nUsage:\nsegment_info.sh SEGMENT_IDENTITY_TABLE DEST_TABLE \n"
	}


if [[ $# -ne 2  ]]
then
    display_usage
    exit 1
fi

SEGMENT_IDENTITY_TABLE=$1
DEST_TABLE=$2

SCHEMA=${ASSETS}/segment_info.schema.json
SQL=${ASSETS}/segment_info.sql.j2
TABLE_DESC=(
  "* Pipeline: ${PIPELINE} ${PIPELINE_VERSION}"
  "* Source: ${SEGMENT_IDENTITY_TABLE}"
  "* Command:"
  "$(basename $0)"
  "$@"
  "Summary table for segments.  One row per segement id. This table can be used to filter out noise segments and to map between ssvid and vesssel_id "
)
TABLE_DESC=$( IFS=$'\n'; echo "${TABLE_DESC[*]}" )

echo "Publishing segment_info to ${DEST_TABLE}..."
echo "${TABLE_DESC}"

jinja2 ${SQL} \
   -D segment_identity=${SEGMENT_IDENTITY_TABLE//:/.} \
   | bq query --max_rows=0 --allow_large_results --replace \
     --destination_table ${DEST_TABLE}

bq update --schema ${SCHEMA} --description "${TABLE_DESC}" ${DEST_TABLE}

echo "  ${DEST} Done."






