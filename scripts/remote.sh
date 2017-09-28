#!/bin/bash

SINK_TABLE=pipe_segment_test_f
JOB_NAME=job-${SINK_TABLE//_/-}

docker-compose run pipeline \
  --source @examples/local.sql  \
  remote \
  --sink world-fishing-827:scratch_paul.$SINK_TABLE \
  --job_name $JOB_NAME \
  --temp_location gs://paul-scratch/$SINK_TABLE \
  --max_num_workers 2 \
  --project world-fishing-827

