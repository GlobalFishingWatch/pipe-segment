#!/bin/bash

docker compose run --entrypoint pipe dev segment \
    --date_range='2024-04-12,2024-04-12' \
    --segmenter_params='{"max_hours": 24}' \
    --in_normalized_messages_table=world-fishing-827.pipe_ais_sources_v20220628.pipe_nmea_normalized_ \
    --out_segmented_messages_table=scratch_tomas_ttl30d.messages_segmented_ \
    --out_segments_table=scratch_tomas_ttl30d.segments_ \
    --fragments_table=scratch_tomas_ttl30d.fragments_ \
    --in_normalized_sat_offset_messages_table=pipe_ais_sources_v20220628.pipe_nmea_normalized_ \
    --out_sat_offsets_table=scratch_tomas_ttl30d.satellite_timing_offsets \
    --in_norad_to_receiver_table=pipe_static.norad_to_receiver_v20230510 \
    --in_sat_positions_table=satellite_positions_v20190208.satellite_positions_one_second_resolution_ \
    --setup_file=./setup.py \
    --labels=environment=production \
    --labels=resource_creator=gcp-composer \
    --labels=project=core_pipeline \
    --labels=version=v3 \
    --labels=step=segment \
    --labels=stage=productive \
    --runner=DirectRunner \
    --project=world-fishing-827 \
    --temp_location=gs://pipe-temp-us-central-ttl7/dataflow_temp \
    --staging_location=gs://pipe-temp-us-central-ttl7/dataflow_staging \
    --ssvid_filter_query='"226013750","226010660","226014030"'
