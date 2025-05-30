#!/bin/bash
set -e

source pipe-tools-utils

PROCESS="segment_vessel_daily"
THIS_SCRIPT_DIR="$( cd "$(dirname "${BASH_SOURCE[0]}")" ; pwd -P )"
source ${THIS_SCRIPT_DIR}/pipeline.sh
ASSETS=${THIS_SCRIPT_DIR}/../assets
ARGS=( \
  PROCESS_DATE \
  WINDOW_DAYS \
  SINGLE_IDENT_MIN_FREQ \
  MOST_COMMON_MIN_FREQ \
  SPOOFING_THRESHOLD \
  SEGMENT_IDENTITY_TABLE \
  DEST_TABLE \
  LABELS \
)
TABLE_ARGS=("SEGMENT_IDENTITY_TABLE" "DEST_TABLE")

################################################################################
# Setup up project to be billed for this process
################################################################################
if [ -z "${BILLING_PROJECT_ID}" ]; then
  BILLING_PROJECT_ID=world-fishing-827
fi
echo "Using billing project id ${BILLING_PROJECT_ID}"

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
  arg_name="${ARGS[$index]}"
  arg_value="${ARG_VALUES[$index]}"
  echo "  ${arg_name}=${arg_value}"
  # Check if the argument is in the TABLE_ARGS list
  if [[ " ${TABLE_ARGS[@]} " =~ " ${arg_name} " ]]; then
       if [[ ! "$arg_value" =~ ^[a-zA-Z0-9_-]+[\.:][a-zA-Z0-9_]+\.[a-zA-Z0-9_]+$ ]]; then
      echo "Error: $arg_name must be in the format project_id[.|:]dataset_id.table_id, got '$arg_value'"
      exit 1
    fi
    # Replace the first dot with a colon
    arg_value="${arg_value/./:}"
  fi
  declare "${arg_name}"="${arg_value}"
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
LABELS_PARAM=$(test -n ${LABELS} && echo "--label ${LABELS//,/ --label }")

echo "Publishing ${PROCESS} to ${DEST_TABLE}..."
jinja2 ${SQL} \
   -D date="${PROCESS_DATE}" \
   -D window_days=${WINDOW_DAYS} \
   -D single_ident_min_freq=${SINGLE_IDENT_MIN_FREQ} \
   -D most_common_min_freq=${MOST_COMMON_MIN_FREQ} \
   -D spoofing_threshold=${SPOOFING_THRESHOLD} \
   -D segment_identity_daily=${SEGMENT_IDENTITY_TABLE//:/.} \
   | bq query --headless --max_rows=0 --allow_large_results --replace \
     ${LABELS_PARAM} --destination_table ${DEST_TABLE} --project_id ${BILLING_PROJECT_ID}

if [ "$?" -ne 0 ]; then
  echo "  Unable to insert records for table ${DEST_TABLE}"
  exit 1
fi

bq update --description "${TABLE_DESC}" ${DEST_TABLE}

echo "DONE ${DEST_TABLE}."
