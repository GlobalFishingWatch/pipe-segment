#!/bin/bash

THIS_SCRIPT_DIR="$( cd "$(dirname "${BASH_SOURCE[0]}")" ; pwd -P )"

# import settings
source ${THIS_SCRIPT_DIR}/setup.cfg


# A script for system testing.   Not guaranteed to work!!!
JOB_NAME=test_pipe_segment_2017_10_24_f


#docker-compose run pipeline \
#  --messages_source bq://world-fishing-827:scratch_paul.normalize_516_ \
#  --first_date 2017-01-01 \
#  --last_date 2017-01-03 \
#  "--where_sql=mmsi in (538006717, 567532000, 477301500)" \
#  --messages_sink bq://world-fishing-827:scratch_paul.${JOB_NAME}_messages_ \
#  --segments_sink bq://world-fishing-827:scratch_paul.${JOB_NAME}_segments \
#  --segmenter_params @examples/segmenter-params.json \
#  --temp_gcs_location gs://paul-scratch/dataflow-temp \
#  local \
#  --project world-fishing-827 \


#  "--where_sql=mmsi in (538006717, 567532000, 477301500)" \

docker-compose run pipeline \
  --messages_source bq://world-fishing-827:scratch_paul.normalize_516_ \
  --first_date 2017-01-01 \
  --last_date 2017-01-03 \
  --include_fields type,mmsi,imo,shipname,shiptype,shiptype_text,callsign,timestamp,lon,lat,speed,course,heading,distance_from_shore,distance_from_port,tagblock_station,gridcode \
  --messages_sink bq://world-fishing-827:scratch_paul.${JOB_NAME}_messages_ \
  --segments_sink bq://world-fishing-827:scratch_paul.${JOB_NAME}_segments \
  --segmenter_params @examples/segmenter-params.json \
  --temp_gcs_location gs://paul-scratch/dataflow-temp \
  remote \
  --project world-fishing-827 \
  --job_name job-${JOB_NAME//_/-} \
  --temp_location gs://paul-scratch/$JOB_NAME \
  --max_num_workers 100 \
  --disk_size_gb 50 \
  --gpsdio_segment_package $SEGMENTER_LOCAL_PACKAGE \
  --pipe_tools_package $PIPE_TOOLS_LOCAL_PACKAGE


#  --messages_source bq://world-fishing-827:scratch_paul.read_write_bigquery_ \

#  --messages_sink ./output/messages \
#  --segments_sink ./output/segments \


#  --messages_source @examples/test.sql \
#  --messages_schema @examples/test-schema.json \

#docker-compose run pipeline \
#  --messages_source @examples/1-month.sql \
#  --messages_schema @examples/messages-schema.json \
#  --messages_sink bq://world-fishing-827:scratch_paul.${JOB_NAME}_messages \
#  --segments_sink bq://world-fishing-827:scratch_paul.${JOB_NAME}_segments \
#  --segmenter_params @examples/segmenter-params.json \
#  --sink_write_disposition WRITE_TRUNCATE \
#  remote \
#  --job_name job-${JOB_NAME//_/-} \
#  --temp_location gs://paul-scratch/$JOB_NAME \
#  --max_num_workers 100 \
#  --disk_size_gb 50 \
#  --project world-fishing-827 \
#  --segmenter_local_package $SEGMENTER_LOCAL_PACKAGE

