#!/bin/bash

docker compose run --rm --entrypoint pipe dev segment_identity \
	--date_range='2025-01-01,2025-01-01' \
	--source_segments=bq://world-fishing-827:scratch_matias_ttl7d.segments \
	--source_fragments=bq://world-fishing-827:scratch_matias_ttl7d.fragments \
	--dest_segment_identity=bq://world-fishing-827:scratch_matias_ttl7d.segment_identity_daily \
	--setup_file=./setup.py \
	--sdk_container_image=gcr.io/world-fishing-827/github.com/globalfishingwatch/pipe-segment/worker:v4.2.3 \
	--labels=environment=develop \
	--labels=resource_creator=matias \
	--labels=project=core_pipeline \
	--labels=version=v3 \
	--labels=step=segment \
	--labels=stage=productive \
	--runner=DirectRunner \
	--project=world-fishing-827 \
	--temp_location=gs://pipe-temp-us-central-ttl7/dataflow_temp \
	--staging_location=gs://pipe-temp-us-central-ttl7/dataflow_staging
