#!/bin/bash

THIS_SCRIPT_DIR="$( cd "$(dirname "${BASH_SOURCE[0]}")" ; pwd -P )"

# import settings
source ${THIS_SCRIPT_DIR}/test-config.sh

JOB_NAME=test_pipe_segment_2017_12_12

display_usage() {
	echo "Available Commands"
	echo "  load        load sample data into bigquery"
	echo "  local       run the segmenter locally"
	echo "  remote      run the segmenter in dataflow"
	}


if [[ $# -le 0 ]]
then
    display_usage
    exit 1
fi

SAMPLE_DATA=${THIS_SCRIPT_DIR}/../sample_data
DS="2017-09-21 2017-09-22 2017-09-23"
DS_NODASH="20170921 20170922 20170923"

case $1 in

  load)
    for DT in ${DS_NODASH}; do
        bq load \
          --source_format=NEWLINE_DELIMITED_JSON \
          --replace \
          ${PROJECT_ID}:${PIPELINE_DATASET}.${JOB_NAME}_source_${DT} \
          ${SAMPLE_DATA}/messages_${DT}.json \
          ${SAMPLE_DATA}/messages-schema.json
    done
    ;;

  local)
    for DT in ${DS}; do
        docker-compose run pipe_segment \
          --source bq://${PROJECT_ID}:${PIPELINE_DATASET}.${JOB_NAME}_source_ \
          --date_range ${DT},${DT} \
          --dest bq://${PROJECT_ID}:${PIPELINE_DATASET}.${JOB_NAME}_messages_ \
          --segments bq://${PROJECT_ID}:${PIPELINE_DATASET}.${JOB_NAME}_segments_ \
          --segmenter_params @examples/segmenter-params.json \
          --project ${PROJECT_ID} \
          --temp_location gs://${TEMP_BUCKET_NAME} \
          --log_level=DEBUG

    done
    ;;

  remote)
    docker-compose run pipe_segment \
      --source bq://${PROJECT_ID}:${PIPELINE_DATASET}.${JOB_NAME}_source_ \
      --date_range 2017-09-21,2017-09-21 \
      --dest bq://${PROJECT_ID}:${PIPELINE_DATASET}.${JOB_NAME}_messages_ \
      --segments bq://${PROJECT_ID}:${PIPELINE_DATASET}.${JOB_NAME}_segments_ \
      --segmenter_params @examples/segmenter-params.json \
      --runner=DataflowRunner \
      --project world-fishing-827 \
      --temp_location gs://${TEMP_BUCKET_NAME}/dataflow-temp/ \
      --staging_location=gs://${TEMP_BUCKET_NAME}/dataflow-staging/ \
      --job_name ${JOB_NAME//_/-} \
      --max_num_workers 4 \
      --disk_size_gb 50 \
      --requirements_file=./requirements.txt \
      --setup_file=./setup.py \
      --log_level=DEBUG

    ;;

  *)
    display_usage
    exit 0
    ;;
esac
