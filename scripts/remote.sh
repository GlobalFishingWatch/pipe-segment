#!/bin/bash

THIS_SCRIPT_DIR="$( cd "$(dirname "${BASH_SOURCE[0]}")" ; pwd -P )"

# import settings
source ${THIS_SCRIPT_DIR}/setup.cfg


SINK_TABLE=pipe_segment_v0_9e_test_a
JOB_NAME=job-${SINK_TABLE//_/-}

# NB: SEGMENTER_LOCAL_PACKAGE is defined in setup.cfg

docker-compose run pipeline \
  --messages_source @examples/local.sql  \
  --messages_schema @examples/messages-schema.json \
  --messages_sink bq://world-fishing-827:scratch_paul.${SINK_TABLE}_messages \
  --segments_sink bq://world-fishing-827:scratch_paul.${SINK_TABLE}_segments \
  --segmenter_params @examples/segmenter-params.json \
  --sink_write_disposition WRITE_TRUNCATE \
  remote \
  --job_name $JOB_NAME \
  --temp_location gs://paul-scratch/$JOB_NAME \
  --max_num_workers 4 \
  --disk_size_gb 50 \
  --project world-fishing-827 \
  --segmenter_local_package $SEGMENTER_LOCAL_PACKAGE

