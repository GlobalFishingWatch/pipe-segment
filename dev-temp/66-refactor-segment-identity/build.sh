#!/usr/bin/env bash
# Build an eval dataset in bigquery

set -e

source pipe-tools-utils

THIS_SCRIPT_DIR="$( cd "$(dirname "${BASH_SOURCE[0]}")" ; pwd -P )"
EXEC_DIR=${THIS_SCRIPT_DIR}/../../scripts

display_usage() {
	echo "Available Commands"
	echo "  fetch_messages"
	echo "  segment_identity_daily"
	echo "  segment_vessel_daily"
	echo "  segment_info"
	echo "  vessel_info"
	echo "  segment_vessel"
	echo "  build_all"
	}


if [[ $# -le 0 ]]
then
    display_usage
    exit 1
fi

PROJECT=world-fishing-827
DATASET=66_refactor_seg_ident_ttl100
SOURCE_DATASET=pipe_production_b
START_DATE=2018-08-01
END_DATE=2018-08-03
WINDOW_DAYS=30
SINGLE_IDENT_MIN_FREQ=0.99

DATE_RANGE=${START_DATE},${END_DATE}

MESSAGES_TABLE=messages_segmented_
SEGMENTS_TABLE=segments_
SOURCE_MESSAGES="${PROJECT}:${SOURCE_DATASET}.${MESSAGES_TABLE}"
SOURCE_SEGMENTS="${PROJECT}:${SOURCE_DATASET}.${SEGMENTS_TABLE}"
MESSAGES=${PROJECT}:${DATASET}.${MESSAGES_TABLE}
SEGMENTS=${PROJECT}:${DATASET}.${SEGMENTS_TABLE}
SEGMENT_IDENTITY_DAILY=${PROJECT}:${DATASET}.segment_identity_daily_
SEGMENT_VESSEL_DAILY=${PROJECT}:${DATASET}.segment_vessel_daily_
SEGMENT_INFO=${PROJECT}:${DATASET}.segment_info
VESSEL_INFO=${PROJECT}:${DATASET}.vessel_info
SEGMENT_VESSEL=${PROJECT}:${DATASET}.segment_vessel


case $1 in
  fetch_messages)
    # Copy source data into working dataset
    xdaterange ${THIS_SCRIPT_DIR}/fetch_messages.sh ${DATE_RANGE} ${SOURCE_MESSAGES} ${MESSAGES}
    xdaterange ${THIS_SCRIPT_DIR}/fetch_messages.sh ${DATE_RANGE} ${SOURCE_SEGMENTS} ${SEGMENTS}
  ;;

  segment_identity_daily)
    xdaterange ${EXEC_DIR}/segment_identity_daily.sh ${DATE_RANGE} ${MESSAGES} ${SEGMENTS} ${SEGMENT_IDENTITY_DAILY}
  ;;

  segment_vessel_daily)
    xdaterange ${EXEC_DIR}/segment_vessel_daily.sh ${DATE_RANGE} ${WINDOW_DAYS} ${SINGLE_IDENT_MIN_FREQ}  \
      ${SEGMENT_IDENTITY_DAILY} ${SEGMENT_VESSEL_DAILY}
  ;;

  segment_info)
    ${EXEC_DIR}/segment_info.sh ${SEGMENT_IDENTITY_DAILY} ${SEGMENT_INFO}
  ;;

  vessel_info)
    ${EXEC_DIR}/vessel_info.sh ${SEGMENT_IDENTITY_DAILY} ${SEGMENT_VESSEL_DAILY} ${VESSEL_INFO}
  ;;

  segment_vessel)
    ${EXEC_DIR}/segment_vessel.sh  ${SEGMENT_VESSEL_DAILY} ${SEGMENT_VESSEL}
  ;;

  build_all)
    ${THIS_SCRIPT_DIR}/build.sh fetch_messages
    ${THIS_SCRIPT_DIR}/build.sh segment_identity_daily
    ${THIS_SCRIPT_DIR}/build.sh segment_vessel_daily
    ${THIS_SCRIPT_DIR}/build.sh segment_info
    ${THIS_SCRIPT_DIR}/build.sh vessel_info
    ${THIS_SCRIPT_DIR}/build.sh segment_vessel
  ;;
  *)
    display_usage
    exit 0
    ;;
esac
