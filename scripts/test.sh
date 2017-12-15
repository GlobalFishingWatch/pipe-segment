#!/bin/bash

THIS_SCRIPT_DIR="$( cd "$(dirname "${BASH_SOURCE[0]}")" ; pwd -P )"

# import settings
source ${THIS_SCRIPT_DIR}/test-config.sh

JOB_NAME=test_pipe_segment_2017_12_12

display_usage() {
	echo "Available Commands"
	echo "  local       run the segmenter locally"
	echo "  remote      run the segmenter in dataflow"
	}


if [[ $# -le 0 ]]
then
    display_usage
    exit 1
fi


case $1 in

  local)
    docker-compose run pipe_segment \
      --source @examples/local.sql \
      --source_schema @examples/messages-schema.json \
      --dest bq://${PROJECT_ID}:${PIPELINE_DATASET}.${JOB_NAME}_messages_ \
      --segments bq://${PROJECT_ID}:${PIPELINE_DATASET}.${JOB_NAME}_segments_ \
      --temp_location gs://${TEMP_BUCKET_NAME} \
      --project ${PROJECT_ID} \
      --no_pipeline_type_check
    ;;

  remote)

    ;;

  *)
    display_usage
    exit 0
    ;;
esac
