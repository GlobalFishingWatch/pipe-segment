#!/bin/bash
# Usage:
# ./example_segment.sh scratch_output
if [ -z $1 ]; then grep "^##" $(dirname $0)/$(basename $0); exit 1; else DATASET_OUT=$1; fi
echo "Output dataset ${DATASET_OUT}."

docker compose run --rm --entrypoint pipe dev segment_vessel_daily \
    --date_range='2025-01-01,2025-01-01' \
    --source_segment_identity=${DATASET_OUT}.internal__segment_identity_daily \
    --destination=${DATASET_OUT}.internal__segment_vessel_daily \
    --labels=environment=develop \
    --labels=resource_creator=local_example \
    --labels=project=core_pipeline \
    --labels=version=v3 \
    --labels=step=segment \
    --labels=stage=productive \
    --project=world-fishing-827
