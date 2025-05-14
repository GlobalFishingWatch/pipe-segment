#!/bin/bash
# Usage:
# ./example_segment.sh scratch_output
if [ -z $1 ]; then grep "^##" $(dirname $0)/$(basename $0); exit 1; else DATASET_OUT=$1; fi
echo "Output dataset ${DATASET_OUT}."

docker compose run --rm dev segment_identity_daily \
  --date_range='2025-01-01,2025-01-01' \
  --source_segments=bq://world-fishing-827:${DATASET_OUT}.segments \
  --source_fragments=bq://world-fishing-827:${DATASET_OUT}.fragments \
  --dest_segment_identity=bq://world-fishing-827:${DATASET_OUT}.segment_identity_daily \
  --setup_file=./setup.py \
  --sdk_container_image=gcr.io/world-fishing-827/github.com/globalfishingwatch/pipe-segment/worker:v4.4.1 \
  --labels=environment=develop \
  --labels=resource_creator=local_example \
  --labels=project=core_pipeline \
  --labels=version=v3 \
  --labels=step=segment \
  --labels=stage=productive \
  --runner=dataflow \
  --wait_for_job \
  --project=world-fishing-827 \
  --temp_location=gs://pipe-temp-us-central-ttl7/dataflow_temp \
  --staging_location=gs://pipe-temp-us-central-ttl7/dataflow_staging \
  --region=us-central1 \
  --max_num_workers=600 \
  --worker_machine_type=e2-standard-4 \
  --disk_size_gb=50 \
  --job_name=test-segment-segment-identity--20250101 \
  --experiments=use_runner_v2 \
  --no_use_public_ips \
  --network=gfw-internal-network \
  --subnetwork=regions/us-central1/subnetworks/gfw-internal-us-central1
