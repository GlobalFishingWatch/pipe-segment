#!/bin/bash
# Usage:
# ./example_segment.sh scratch_output
if [ -z $1 ]; then grep "^##" $(dirname $0)/$(basename $0); exit 1; else DATASET_OUT=$1; fi
echo "Output dataset ${DATASET_OUT}."

docker compose run --rm --entrypoint pipe dev segment_identity \
  --date_range='2025-01-01,2025-01-01' \
  --source_segments=bq://world-fishing-827:${DATASET_OUT}.internal__segments \
  --source_fragments=bq://world-fishing-827:${DATASET_OUT}.internal__fragments \
  --dest_segment_identity=bq://world-fishing-827:${DATASET_OUT}.internal__segment_identity_daily \
  --setup_file=./setup.py \
  --labels=environment=develop \
  --labels=resource_creator=local_example \
  --labels=project=core_pipeline \
  --labels=version=v3 \
  --labels=step=segment \
  --labels=stage=productive \
  --runner=DirectRunner \
  --project=world-fishing-827 \
  --temp_location=gs://pipe-temp-us-central-ttl7/dataflow_temp \
  --job_name=test-segment-segment-identity--20250101 \
  --staging_location=gs://pipe-temp-us-central-ttl7/dataflow_staging
