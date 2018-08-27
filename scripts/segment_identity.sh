#!/bin/bash
set -e

source pipe-tools-utils

THIS_SCRIPT_DIR="$( cd "$(dirname "${BASH_SOURCE[0]}")" ; pwd -P )"

ASSETS=${THIS_SCRIPT_DIR}/../assets
source ${THIS_SCRIPT_DIR}/pipeline.sh

display_usage() {
	echo -e "\nUsage:\nsegment_identity.sh INDENTITY_MESSAGES SEGMENTS START_DATE END_DATE DEST \n"
	}


if [[ $# -ne 5  ]]
then
    display_usage
    exit 1
fi

INDENTITY_MESSAGES=$1
SEGMENTS=$2
START_DATE=$3
END_DATE=$4
DEST=$5

START_YYYYMMDD=$(yyyymmdd ${START_DATE})
END_YYYYMMDD=$(yyyymmdd ${END_DATE})


SQL=${ASSETS}/segment_identity.sql.j2
TABLE_DESC=(
  "DEPRECATED"
  " "
  "* Pipeline: ${PIPELINE} ${PIPELINE_VERSION}"
  "* Source: ${INDENTITY_MESSAGES}"
  "* Command:"
  "$(basename $0)"
  "$@"
)
TABLE_DESC=$( IFS=$'\n'; echo "${TABLE_DESC[*]}" )

echo "Publishing segment_identity to ${DEST}..."
echo "  ${START_DATE} to ${END_DATE}"
echo "${TABLE_DESC}"

jinja2 ${SQL} \
   -D identity_messages_monthly=${INDENTITY_MESSAGES//:/.} \
   -D segments=${SEGMENTS//:/.} \
   -D start_yyyymmdd=${START_YYYYMMDD} \
   -D end_yyyymmdd=${END_YYYYMMDD} \
   | bq query --max_rows=0 --allow_large_results --replace \
     --destination_table ${DEST}


echo "  ${DEST} Done."






