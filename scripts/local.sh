#!/bin/bash

docker-compose run pipeline \
  --source @examples/local.sql \
 local \
 --project world-fishing-827
