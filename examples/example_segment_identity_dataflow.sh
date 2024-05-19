#!/bin/bash

docker compose run dev segment_identity_daily \
    --date_range='2024-04-23,2024-04-23' \
    --source_segments=bq://world-fishing-827:scratch_tomas_ttl30d.segments_ \
    --source_fragments=bq://world-fishing-827:scratch_tomas_ttl30d.fragments_ \
    --dest_segment_identity=bq://world-fishing-827:scratch_tomas_ttl30d.segment_identity_daily_ \
    --setup_file=./setup.py \
    --sdk_container_image=gcr.io/world-fishing-827/github.com/globalfishingwatch/pipe-segment/worker:v4.2.3 \
    --labels=environment=production \
    --labels=resource_creator=gcp-composer \
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
    --worker_machine_type=custom-1-65536-ext \
    --disk_size_gb=50 \
    --job_name=core-ais-v3-staging-daily--segment-segment-identity-daily--20240423 \
    --experiments=use_runner_v2 \
    --no_use_public_ips \
    --network=gfw-internal-network \
    --subnetwork=regions/us-central1/subnetworks/gfw-internal-us-central1