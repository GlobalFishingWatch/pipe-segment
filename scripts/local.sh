#!/bin/bash

docker-compose run pipeline \
 local \
  --sourcequery @examples/local.sql \
  --project world-fishing-827 \
  --sink ./local-output
