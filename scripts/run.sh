#!/usr/bin/env bash
set -e

THIS_SCRIPT_DIR="$( cd "$(dirname "${BASH_SOURCE[0]}")" ; pwd -P )"

display_usage() {
	echo "Available Commands"
	echo "  segment                     run the segmenter in dataflow"
	echo "  segment_identity_daily      generate daily summary of identity messages"
	echo "                              per segment"
	echo "  segment_vessel_daily        generate daily vessel_ids per segment"
	echo "  segment_info                create a segment_info table with one row"
	echo "                              per segment"
	echo "  vessel_info                 create a vessel_info table with one row"
	echo "                              per vessel_id"
  echo "  segment_vessel              Create a many-to-many table mapping between"
  echo "                              segment_id, vessel_id and ssvid"
}


if [[ $# -le 0 ]]
then
    display_usage
    exit 1
fi


case $1 in
  segment_identity_daily)
    xdaterange ${THIS_SCRIPT_DIR}/segment_identity_daily.sh "${@:2}"
  ;;

  segment_vessel_daily)
    xdaterange ${THIS_SCRIPT_DIR}/segment_vessel_daily.sh "${@:2}"
  ;;

  segment_info)
    ${THIS_SCRIPT_DIR}/segment_info.sh "${@:2}"
  ;;

  vessel_info)
    ${THIS_SCRIPT_DIR}/vessel_info.sh "${@:2}"
  ;;

  segment_vessel)
    ${THIS_SCRIPT_DIR}/segment_vessel.sh "${@:2}"
  ;;

  segment)
    python -m pipe_segment "${@:2}"
    ;;

  *)
    display_usage
    exit 1
    ;;
esac
