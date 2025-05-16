#!/bin/bash
## Usage:
## ./example_segment.sh scratch_output
if [ -z $1 ]; then grep "^##" $(dirname $0)/$(basename $0); exit 1; else DATASET_OUT=$1; fi
echo "Output dataset ${DATASET_OUT}."

docker compose run --rm dev segment \
  --date_range='2025-01-01,2025-01-01' \
  --segmenter_params='{"max_hours": 24}' \
  --in_normalized_messages_table=world-fishing-827.pipe_ais_sources_v20220628.pipe_nmea_normalized_ \
  --out_segmented_messages_table=${DATASET_OUT}.messages_segmented \
  --out_segments_table=${DATASET_OUT}.segments \
  --fragments_table=${DATASET_OUT}.fragments \
  --in_normalized_sat_offset_messages_table=pipe_ais_sources_v20220628.pipe_nmea_normalized_ \
  --out_sat_offsets_table=${DATASET_OUT}.satellite_timing_offsets \
  --in_norad_to_receiver_table=pipe_static.norad_to_receiver_v20230510 \
  --in_sat_positions_table=satellite_positions_v20190208.satellite_positions_one_second_resolution_ \
  --setup_file=./setup.py \
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
  --ssvid_filter_query='"9921118512","993660556","4402609","311046100","4403330","2614100","993692032"' \
  --sdk_container_image=gcr.io/world-fishing-827/github.com/globalfishingwatch/pipe-segment:v4.4.1 \
  --region=us-central1 \
  --max_num_workers=600 \
  --worker_machine_type=e2-standard-4 \
  --disk_size_gb=50 \
  --job_name=test-segment-segment--20250101 \
  --experiments=use_runner_v2 \
  --no_use_public_ips \
  --network=gfw-internal-network \
  --subnetwork=regions/us-central1/subnetworks/gfw-internal-us-central1
