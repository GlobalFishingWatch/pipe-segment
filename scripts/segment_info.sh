#!/bin/bash
set -e

source pipe-tools-utils

PROCESS="segment_info"
THIS_SCRIPT_DIR="$( cd "$(dirname "${BASH_SOURCE[0]}")" ; pwd -P )"
source ${THIS_SCRIPT_DIR}/pipeline.sh
ASSETS=${THIS_SCRIPT_DIR}/../assets
ARGS=( \
  SEGMENT_IDENTITY_TABLE \
  SEGMENT_VESSEL_DAILY \
  MOST_COMMON_MIN_FREQ \
  DEST_TABLE \
  LABELS \
)

################################################################################
# Validate and extract arguments
################################################################################
display_usage() {
  ARG_NAMES=$(echo "${ARGS[*]}")
  echo -e "\nUsage:\n$0 $ARG_NAMES\n"
}

if [[ $# -ne ${#ARGS[@]} ]]
then
    display_usage
    exit 1
fi

echo "Running $0"
ARG_VALUES=("$@")
for index in ${!ARGS[*]}; do
  echo "  ${ARGS[$index]}=${ARG_VALUES[$index]}"
  declare "${ARGS[$index]}"="${ARG_VALUES[$index]}"
done

################################################################################
# Force that the destination table exists
################################################################################
echo "Ensuring table ${DEST_TABLE} exists"
TABLE_DESC=(
  "* Pipeline: ${PIPELINE} ${PIPELINE_VERSION}"
  "* Source: ${SOURCE_TABLE}"
  "* Command:"
  "$(basename $0)"
  "$@"
)
TABLE_DESC=$( IFS=$'\n'; echo "${TABLE_DESC[*]}" )
SCHEMA=${ASSETS}/${PROCESS}.schema.json
bq mk --force \
  --description "${TABLE_DESC}" \
  ${DEST_TABLE} \
  ${SCHEMA}

if [ "$?" -ne 0 ]; then
  echo "  Unable to create table ${DEST_TABLE}"
  exit 1
fi
echo "  Table ${DEST_TABLE} exists"

################################################################################
# Generate data
################################################################################
SQL=${ASSETS}/${PROCESS}.sql.j2
LABELS_PARAM=$(test -n ${LABELS} && echo "--label ${LABELS//,/ --label }")

echo "Publishing ${PROCESS} to ${DEST_TABLE}..."
jinja2 ${SQL} \
   -D segment_identity_daily=${SEGMENT_IDENTITY_TABLE//:/.} \
   -D segment_vessel_daily=${SEGMENT_VESSEL_DAILY//:/.} \
   -D most_common_min_freq=${MOST_COMMON_MIN_FREQ} \
   | bq query --headless --max_rows=0 --allow_large_results --replace \
     ${LABELS_PARAM} --destination_table ${DEST_TABLE}

if [ "$?" -ne 0 ]; then
  echo "  Unable to insert records for table ${DEST_TABLE}"
  exit 1
fi


bq update --description "${TABLE_DESC}" ${DEST_TABLE}

echo "DONE ${DEST_TABLE}."
