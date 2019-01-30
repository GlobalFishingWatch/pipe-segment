#!/bin/bash
set -e

source pipe-tools-utils

THIS_SCRIPT_DIR="$( cd "$(dirname "${BASH_SOURCE[0]}")" ; pwd -P )"

ASSETS=${THIS_SCRIPT_DIR}/../assets
source ${THIS_SCRIPT_DIR}/pipeline.sh

PROCESS=$(basename $0 .sh)
ARGS=( PROCESS_DATE WINDOW_DAYS SINGLE_IDENT_MIN_FREQ MOST_COMMON_MIN_FREQ SPOOFING_THRESHOLD SEGMENT_IDENTITY_TABLE DEST_TABLE )
SCHEMA=${ASSETS}/${PROCESS}.schema.json
SQL=${ASSETS}/${PROCESS}.sql.j2
TABLE_DESC=(
  "Daily vessel_id assignement for each segment based on most common identity values occuring in a sliding date range of width WINDOW_DAYS"
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
DEST_TABLE=${DEST_TABLE}$(yyyymmdd ${PROCESS_DATE})


echo "Publishing ${PROCESS} to ${DEST_TABLE}..."
echo ""
echo "Table Description" | indent
echo "${TABLE_DESC}" | indent
echo ""
echo "Executing query..." | indent

jinja2 ${SQL} \
   -D date="${PROCESS_DATE}" \
   -D window_days=${WINDOW_DAYS} \
   -D single_ident_min_freq=${SINGLE_IDENT_MIN_FREQ} \
   -D most_common_min_freq=${MOST_COMMON_MIN_FREQ} \
   -D spoofing_threshold=${SPOOFING_THRESHOLD} \
   -D segment_identity_daily=${SEGMENT_IDENTITY_TABLE//:/.} \
   | bq query --headless --max_rows=0 --allow_large_results --replace \
     --destination_table ${DEST_TABLE}

echo ""
bq update --schema ${SCHEMA} --description "${TABLE_DESC}" ${DEST_TABLE}

echo ""
echo "DONE ${DEST_TABLE}."
