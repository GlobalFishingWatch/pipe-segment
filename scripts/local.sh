#!/bin/bash

#docker-compose run pipeline \
#  --messages_source @examples/local.sql \
#  --messages_sink ./output/messages \
#  --segments_sink ./output/segments \
#  local \
#  --project world-fishing-827 \

#docker-compose run pipeline \
#  --window=60 \
#  --messages_source @examples/local.sql \
#  --messages_sink bq://world-fishing-827:scratch_paul.test_2017_10_05a_messages \
#  --segments_sink bq://world-fishing-827:scratch_paul.test_2017_10_05a_segments \
#  --sink_write_disposition WRITE_TRUNCATE \
#  local \
#  --project world-fishing-827 \

docker-compose run pipeline \
  --messages_source @examples/local.sql \
  --messages_sink ./output/messages \
  --segments_sink ./output/segments \
  --segmenter_params @examples/segmenter-params.json \
  local \
  --project world-fishing-827 \
