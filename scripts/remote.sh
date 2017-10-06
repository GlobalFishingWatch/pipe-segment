#!/bin/bash

THIS_SCRIPT_DIR="$( cd "$(dirname "${BASH_SOURCE[0]}")" ; pwd -P )"

# import settings
source ${THIS_SCRIPT_DIR}/setup.cfg


SINK_TABLE=pipe_segment_v0_8_2015_01_test_a
JOB_NAME=job-${SINK_TABLE//_/-}

# NB: SEGMENTER_LOCAL_PACKAGE is defined in setup.cfg

docker-compose run pipeline \
  --messages_source @examples/local.sql  \
  --messages_sink bq://world-fishing-827:scratch_paul.${SINK_TABLE}_messages \
  --segments_sink bq://world-fishing-827:scratch_paul.${SINK_TABLE}_segments \
  --sink_write_disposition WRITE_TRUNCATE \
  remote \
  --job_name $JOB_NAME \
  --temp_location gs://paul-scratch/$SINK_TABLE \
  --max_num_workers 4 \
  --project world-fishing-827 \
  --segmenter_local_package $SEGMENTER_LOCAL_PACKAGE

