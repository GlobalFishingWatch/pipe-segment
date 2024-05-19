#!/bin/bash

docker compose run dev segment_identity_daily \
	--date_range='2024-04-12,2024-04-12' \
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
	--runner=direct \
    --project=world-fishing-827 \
    --temp_location=gs://pipe-temp-us-central-ttl7/dataflow_temp \
    --staging_location=gs://pipe-temp-us-central-ttl7/dataflow_staging