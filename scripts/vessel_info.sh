#!/bin/bash
set -e

source pipe-tools-utils

THIS_SCRIPT_DIR="$( cd "$(dirname "${BASH_SOURCE[0]}")" ; pwd -P )"

ASSETS=${THIS_SCRIPT_DIR}/../assets
source ${THIS_SCRIPT_DIR}/pipeline.sh

PROCESS=$(basename $0 .sh)
ARGS=( SEGMENT_IDENTITY_TABLE SEGMENT_VESSEL_TABLE DEST_TABLE )
SCHEMA=${ASSETS}/${PROCESS}.schema.json
SQL=${ASSETS}/${PROCESS}.sql.j2
TABLE_DESC=(
  "Comprehensive table of all vessel_ids for all time.  One row per vessel_id"
  ""
  "* Pipeline: ${PIPELINE} ${PIPELINE_VERSION}"
  "* Source: ${SEGMENT_VESSEL_TABLE}"
  "* Command: $(basename $0)"
)

display_usage()
{
  echo -e "\nUsage:\n${PROCESS}.sh ${ARGS[*]} \n"
}

if [[ $# -ne ${#ARGS[@]} ]]
then
    display_usage
    exit 1
fi

ARG_VALUES=("$@")
PARAMS=()
for index in ${!ARGS[*]}; do
  echo ${ARG_VALUES[$index]}
  declare "${ARGS[$index]}"="${ARG_VALUES[$index]}"
  PARAMS+=("${ARGS[$index]}=${ARG_VALUES[$index]}")
done

TABLE_DESC+=(${PARAMS[*]})
TABLE_DESC=$( IFS=$'\n'; echo "${TABLE_DESC[*]}" )


echo "Publishing ${PROCESS} to ${DEST_TABLE}..."
echo ""
echo "Table Description" | indent
echo "${TABLE_DESC}" | indent
echo ""
echo "Executing query..." | indent
jinja2 ${SQL} \
   -D segment_identity_daily=${SEGMENT_IDENTITY_TABLE//:/.} \
   -D segment_vessel_daily=${SEGMENT_VESSEL_TABLE//:/.} \
   | bq query --headless --max_rows=0 --allow_large_results --replace \
     --destination_table ${DEST_TABLE}

echo ""
bq update --schema ${SCHEMA} --description "${TABLE_DESC}" ${DEST_TABLE}

echo ""
echo "DONE ${DEST_TABLE}."
