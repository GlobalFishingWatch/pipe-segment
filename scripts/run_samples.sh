#!/bin/bash

THIS_SCRIPT_DIR="$( cd "$(dirname "${BASH_SOURCE[0]}")" ; pwd -P )"

# import settings
source ${THIS_SCRIPT_DIR}/setup.cfg

SAMPLES_DIR=./scripts/samples
SAMPLES=${SAMPLES_DIR}/*.json

for FILE in ${SAMPLES}
do

    FILE_BASE=`basename ${FILE} .json`
    echo "Processing $FILE_BASE file..."

    JOB_NAME=job-${FILE_BASE//_/-}-a
    SINK_TABLE=v0_9d_${FILE_BASE//-/_}

    docker-compose run pipeline \
      --messages_source @examples/1-month.sql  \
      --messages_schema @examples/messages-schema.json \
      --messages_sink bq://world-fishing-827:scratch_segment.${SINK_TABLE}_messages \
      --segments_sink bq://world-fishing-827:scratch_segment.${SINK_TABLE}_segments \
      --segmenter_params @${FILE} \
      --sink_write_disposition WRITE_TRUNCATE \
      remote \
      --job_name $JOB_NAME \
      --temp_location gs://paul-scratch/$JOB_NAME \
      --max_num_workers 100 \
      --disk_size_gb 50 \
      --project world-fishing-827 \
      --segmenter_local_package $SEGMENTER_LOCAL_PACKAGE

done
