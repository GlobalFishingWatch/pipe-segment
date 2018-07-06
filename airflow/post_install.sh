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
    normalized_tables="normalized_orbcomm_,normalized_spire_" \
    pipeline_bucket="{{ var.value.PIPELINE_BUCKET }}" \
    pipeline_dataset="{{ var.value.PIPELINE_DATASET }}" \
    project_id="{{ var.value.PROJECT_ID }}" \
    segment_identity_table="segment_identity_" \
    segments_table="segments_" \
    segment_info_table="segment_info" \
    source_dataset="{{ var.value.PIPELINE_DATASET }}" \
    temp_bucket="{{ var.value.TEMP_BUCKET }}"  \

echo "Installation Complete"


