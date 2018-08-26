#!/bin/bash
set -e

source pipe-tools-utils

THIS_SCRIPT_DIR="$( cd "$(dirname "${BASH_SOURCE[0]}")" ; pwd -P )"

ASSETS=${THIS_SCRIPT_DIR}/../assets
source ${THIS_SCRIPT_DIR}/pipeline.sh

PROCESS=$(basename $0 .sh)
ARGS=( PROCESS_DATE MESSAGES_TABLE SEGMENTS_TABLE DEST_TABLE )
SCHEMA=${ASSETS}/${PROCESS}.schema.json
SQL=${ASSETS}/${PROCESS}.sql.j2
TABLE_DESC=(
  "Daily summary of identity messages by segment"
  ""
  "* Pipeline: ${PIPELINE} ${PIPELINE_VERSION}"
  "* Source: ${MESSAGES_TABLE}"
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
YYYYMMDD=$(yyyymmdd ${PROCESS_DATE})
DEST_TABLE=${DEST_TABLE}${PROCESS_DATE}


echo "Publishing ${PROCESS} to ${DEST_TABLE}..."
echo ""
echo "Table Description" | indent
echo "${TABLE_DESC}" | indent
echo ""
echo "Executing query..." | indent
jinja2 ${SQL} \
   -D messages=${MESSAGES_TABLE//:/.}${YYYYMMDD} \
   -D segments=${SEGMENTS_TABLE//:/.}${YYYYMMDD} \
   | bq query --headless --max_rows=0 --allow_large_results --replace \
     --destination_table ${DEST_TABLE}  | indent

echo ""
bq update --schema ${SCHEMA} --description "${TABLE_DESC}" ${DEST_TABLE} | indent

echo ""
echo "DONE ${DEST_TABLE}."
