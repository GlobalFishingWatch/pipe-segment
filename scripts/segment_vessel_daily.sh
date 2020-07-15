#!/bin/bash
set -e

source pipe-tools-utils

PROCESS="segment_vessel_daily"
THIS_SCRIPT_DIR="$( cd "$(dirname "${BASH_SOURCE[0]}")" ; pwd -P )"
ASSETS=${THIS_SCRIPT_DIR}/../assets
ARGS=( \
  PROCESS_DATE \
  WINDOW_DAYS \
  SINGLE_IDENT_MIN_FREQ \
  MOST_COMMON_MIN_FREQ \
  SPOOFING_THRESHOLD \
  SEGMENT_IDENTITY_TABLE \
  DEST_TABLE \
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
YYYYMMDD=$(yyyymmdd ${PROCESS_DATE})
DEST_TABLE=${DEST_TABLE}${YYYYMMDD}

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

echo "Publishing ${PROCESS} to ${DEST_TABLE}..."
jinja2 ${SQL} \
   -D date="${PROCESS_DATE}" \
   -D window_days=${WINDOW_DAYS} \
   -D single_ident_min_freq=${SINGLE_IDENT_MIN_FREQ} \
   -D most_common_min_freq=${MOST_COMMON_MIN_FREQ} \
   -D spoofing_threshold=${SPOOFING_THRESHOLD} \
   -D segment_identity_daily=${SEGMENT_IDENTITY_TABLE//:/.} \
   | bq query --headless --max_rows=0 --allow_large_results --replace \
     --destination_table ${DEST_TABLE}

if [ "$?" -ne 0 ]; then
  echo "  Unable to insert records for table ${DEST_TABLE}"
  exit 1
fi

echo "DONE ${DEST_TABLE}."
