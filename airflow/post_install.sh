#!/bin/bash

python $AIRFLOW_HOME/utils/set_default_variables.py \
    --force docker_image=$1 \
    pipe_segment \
    project_id="{{ var.value.PROJECT_ID }}" \
    temp_bucket="{{ var.value.TEMP_BUCKET }}"  \
    pipeline_bucket="{{ var.value.PIPELINE_BUCKET }}" \
    pipeline_dataset="{{ var.value.PIPELINE_DATASET }}" \
    normalized_table="normalized_orbcomm_" \
    messages_table="messages_segmented_" \
    segments_table="segments_" \
    identity_messages_monthly_table="identity_messages_monthly_" \
    segment_identity_table="segment_identity_"

echo "Installation Complete"


