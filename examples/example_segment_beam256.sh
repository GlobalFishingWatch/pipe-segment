#!/bin/bash

docker compose run dev segment \
    --date_range='2024-04-12,2024-04-12' \
    --segmenter_params='{"max_hours": 24}' \
    --source=world-fishing-827.pipe_ais_sources_v20220628.pipe_nmea_normalized_ \
    --msg_dest=scratch_tomas_ttl30d.messages_segmented_beam256_ \
    --segment_dest=scratch_tomas_ttl30d.segments_beam256_ \
    --fragment_tbl=scratch_tomas_ttl30d.fragments_beam256_ \
    --sat_source=pipe_ais_sources_v20220628.pipe_nmea_normalized_ \
    --sat_offset_dest=scratch_tomas_ttl30d.satellite_timing_offsets_beam256 \
    --norad_to_receiver_tbl=pipe_static.norad_to_receiver_v20230510 \
    --sat_positions_tbl=satellite_positions_v20190208.satellite_positions_one_second_resolution_ \
    --setup_file=./setup.py \
    --labels=environment=production \
    --labels=resource_creator=gcp-composer \
    --labels=project=core_pipeline \
    --labels=version=v3 \
    --labels=step=segment \
    --labels=stage=productive \
    --runner=direct \
    --project=world-fishing-827 \
    --temp_location=gs://pipe-temp-us-central-ttl7/dataflow_temp \
    --staging_location=gs://pipe-temp-us-central-ttl7/dataflow_staging \
    --ssvid_filter_query='"226013750","226010660","226014030"'