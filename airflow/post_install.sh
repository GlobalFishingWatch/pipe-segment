#!/bin/bash

THIS_SCRIPT_DIR="$( cd "$(dirname "${BASH_SOURCE[0]}")" ; pwd -P )"

airflow variables --get pipe_segment || NOT_FOUND=$? && true

if [[ $NOT_FOUND ]]; then
    airflow variables -i $THIS_SCRIPT_DIR/variables.json
fi