#!/bin/bash

THIS_SCRIPT_DIR="$( cd "$(dirname "${BASH_SOURCE[0]}")" ; pwd -P )"

# import settings
source ${THIS_SCRIPT_DIR}/setup.cfg


SINK_TABLE=pipe_segment_v0_8_2015_01
JOB_NAME=job-${SINK_TABLE//_/-}

# NB: SEGMENTER_LOCAL_PACKAGE is defined in setup.cfg

docker-compose run pipeline \
  remote \
  --sourcequery @examples/1-month.sql  \
  --sink world-fishing-827:scratch_paul.$SINK_TABLE \
  --sink_write_disposition WRITE_TRUNCATE \
  --job_name $JOB_NAME \
  --temp_location gs://paul-scratch/$SINK_TABLE \
  --max_num_workers 100 \
  --project world-fishing-827 \
  --segmenter_local_package $SEGMENTER_LOCAL_PACKAGE

