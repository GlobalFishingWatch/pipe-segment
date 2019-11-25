#!/bin/bash

THIS_SCRIPT_DIR="$( cd "$(dirname "${BASH_SOURCE[0]}")" ; pwd -P )"

python $AIRFLOW_HOME/utils/set_default_variables.py \
    --force docker_image=$1 \
    pipe_segment \
    dag_install_path="${THIS_SCRIPT_DIR}" \
    dataflow_runner="DataflowRunner" \
    docker_run="{{ var.value.DOCKER_RUN }}" \
    identity_messages_monthly_table="identity_messages_monthly_" \
    messages_table="messages_segmented_" \
    dataflow_max_num_workers="100" \
    dataflow_disk_size_gb="50" \
    dataflow_machine_type="custom-1-15360-ext" \
    source_tables="normalized_orbcomm_,normalized_spire_" \
    pipeline_bucket="{{ var.value.PIPELINE_BUCKET }}" \
    pipeline_dataset="{{ var.value.PIPELINE_DATASET }}" \
    project_id="{{ var.value.PROJECT_ID }}" \
    segment_identity_table="segment_identity_" \
    segment_identity_daily_table="segment_identity_daily_" \
    segment_info_table="segment_info" \
    segment_vessel_daily_table="segment_vessel_daily_" \
    segment_vessel_table="segment_vessel" \
    segments_table="segments_" \
    legacy_segment_v1_table="legacy_segment_v1_" \
    spoofing_threshold="10" \
    single_ident_min_freq="0.99" \
    most_common_min_freq="0.05" \
    source_dataset="{{ var.value.PIPELINE_DATASET }}" \
    vessel_info_table="vessel_info" \
    temp_bucket="{{ var.value.TEMP_BUCKET }}"  \
    window_days="30"  \


echo "Installation Complete"


