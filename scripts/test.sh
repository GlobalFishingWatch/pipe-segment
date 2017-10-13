#!/bin/bash

THIS_SCRIPT_DIR="$( cd "$(dirname "${BASH_SOURCE[0]}")" ; pwd -P )"

# import settings
source ${THIS_SCRIPT_DIR}/setup.cfg


# A script for system testing.   Not guaranteed to work!!!
JOB_NAME=test_pipe_segment_2017_10_13c

#docker-compose run pipeline \
#  --messages_source @examples/test.sql \
#  --messages_schema @examples/test-schema.json \
#  --messages_sink bq://world-fishing-827:scratch_paul.${JOB_NAME}_messages \
#  --segments_sink bq://world-fishing-827:scratch_paul.${JOB_NAME}_segments \
#  --segmenter_params @examples/segmenter-params.json \
#  local \
#  --project world-fishing-827 \


#  --messages_sink ./output/messages \
#  --segments_sink ./output/segments \


#  --messages_source @examples/test.sql \
#  --messages_schema @examples/test-schema.json \

docker-compose run pipeline \
  --messages_source @examples/1-day.sql \
  --messages_schema @examples/messages-schema.json \
  --messages_sink bq://world-fishing-827:scratch_paul.${JOB_NAME}_messages \
  --segments_sink bq://world-fishing-827:scratch_paul.${JOB_NAME}_segments \
  --segmenter_params @examples/segmenter-params.json \
  --sink_write_disposition WRITE_TRUNCATE \
  remote \
  --job_name job-${JOB_NAME//_/-} \
  --temp_location gs://paul-scratch/$JOB_NAME \
  --max_num_workers 100 \
  --disk_size_gb 50 \
  --project world-fishing-827 \
  --segmenter_local_package $SEGMENTER_LOCAL_PACKAGE

