#!/bin/bash

THIS_SCRIPT_DIR="$( cd "$(dirname "${BASH_SOURCE[0]}")" ; pwd -P )"

# import settings
source ${THIS_SCRIPT_DIR}/setup.cfg


SINK_TABLE=pipe_segment_test_g
JOB_NAME=job-${SINK_TABLE//_/-}

# NB: SEGMENTER_LOCAL_PACKAGE is exported by setup.sh so that must be run first

docker-compose run pipeline \
  --source @examples/local.sql  \
  remote \
  --sink world-fishing-827:scratch_paul.$SINK_TABLE \
  --job_name $JOB_NAME \
  --temp_location gs://paul-scratch/$SINK_TABLE \
  --max_num_workers 2 \
  --project world-fishing-827 \
  --segmenter_local_package $SEGMENTER_LOCAL_PACKAGE
