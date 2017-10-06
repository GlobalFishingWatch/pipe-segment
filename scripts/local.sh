#!/bin/bash

docker-compose run pipeline \
  --window=60 \
  local \
  --sourcequery @examples/local.sql \
  --project world-fishing-827 \
  --sink ./local-output
