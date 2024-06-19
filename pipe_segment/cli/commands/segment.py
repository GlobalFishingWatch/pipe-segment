import json
import logging
import argparse

from pipe_segment import pipeline
from pipe_segment.cli.commands.base import Command

logger = logging.getLogger(__name__)


EXAMPLE_SEGMENTER_PARAMS = {
    "max_hours": 24,
    "max_speed": 30,
    "noise_dist": 0,
    "reported_speed_multiplier": 1.1,
    "max_speed_multiplier": 15,
    "max_speed_exponent": 1.3,
}

EXAMPLE_MERGE_PARAMS = dict(buffer_hours=0.5)


class Segment(Command):
    NAME = "segment"

    HELP = "segmenter pipeline."
    HELP_SOURCE = "Table to read normalized messages."
    HELP_MSG_DEST = "Table to write segmented messages."
    HELP_FRAGMENT_TBL = "Table to read and write fragments."
    HELP_SEGMENT_DEST = "Table to write segments-days."
    HELP_SAT_SOURCE = "Table, query or file to read normalized messages. Subset of `source`."
    HELP_NORAD = "Table that links NORAD IDs and receivers."
    HELP_SAT_POSITIONS = "Table with distance to satellite by receiver at 1s resolution."
    HELP_SAT_OFFSET_DEST = "Table to write satellite offsets to."
    HELP_BAD_HOUR = "Hours on either side of an hour with bad satellite timing to suppress."
    HELP_MAX_OFFSET = "Max. offset (in seconds) of a satellite clock before we drop its messages."
    HELP_DATE_RANGE = "Range of dates to read from source. Format 'YYYY-MM-DD,YYYY-MM-DD'."
    HELP_WAIT = "Wait until the job finishes before returning."

    HELP_SEGMENTER_PARAMS = (
        "JSON object with fragmenter parameters, or filepath @path/to/file.json. "
        f"For Example: \n {json.dumps(EXAMPLE_SEGMENTER_PARAMS)}"
    )

    HELP_MERGE_PARAMS = (
        "JSON object with fragmenter parameters, or filepath @path/to/file.json. "
        f"For Example: \n {json.dumps(EXAMPLE_MERGE_PARAMS)}"
    )
    HELP_SSVID_FILTER = (
        "Query that returns a list of ssvid to trim the sourced data down to. "
        "Note that the returned list is used in memory so should not be too large. "
        "This meant for testing purposes. If tempted to use for production, "
        "more work should be done so that the data is pruned on the way in."
    )

    HELP_BINS_PER_DAY = "Amount of containers per day to tag fragments and messages."

    EPILOG = "Example: pipe segment --help"

    @classmethod
    def add_to_subparsers(cls, subparsers):
        p = subparsers.add_parser(
            cls.NAME, help=cls.HELP, epilog=cls.EPILOG, formatter_class=cls.formatter())

        p.set_defaults(func=cls.run)

        required = p.add_argument_group("Required")
        add = required.add_argument
        add("--source", required=True, metavar='\b', help=cls.HELP_SOURCE)
        add("--msg_dest", required=True, metavar='\b', help=cls.HELP_MSG_DEST)
        add("--fragment_tbl", required=True, metavar='\b', help=cls.HELP_FRAGMENT_TBL)
        add("--segment_dest", required=True, metavar='\b', help=cls.HELP_SEGMENT_DEST)

        optional = p.add_argument_group("Optional")
        add = optional.add_argument
        add("--sat_source", metavar=' ', help=cls.HELP_SAT_SOURCE)
        add("--norad_to_receiver_tbl", metavar=' ', help=cls.HELP_NORAD)
        add("--sat_positions_tbl", metavar=' ', help=cls.HELP_SAT_POSITIONS)
        add("--sat_offset_dest", metavar=' ', help=cls.HELP_SAT_OFFSET_DEST)
        add("--bad_hour_padding", type=int, default=1, metavar=' ', help=cls.HELP_BAD_HOUR)
        add("--max_timing_offset_s", type=int, default=30, metavar=' ', help=cls.HELP_MAX_OFFSET)
        add("--date_range", metavar=' ', help=cls.HELP_DATE_RANGE)
        add("--wait_for_job", default=False, action="store_true", help=cls.HELP_WAIT)
        add("--segmenter_params", default="{}", metavar=' ', help=cls.HELP_SEGMENTER_PARAMS)
        add("--merge_params", default="{}", metavar=' ', help=cls.HELP_MERGE_PARAMS)
        add("--ssvid_filter_query", metavar=' ', help=cls.HELP_SSVID_FILTER)
        add("--bins_per_day", default=4, metavar=' ', help=cls.HELP_BINS_PER_DAY)

    @classmethod
    def run(cls, args, extra_args):
        logger.info("Running pipe segment command...")
        pipeline.run(args, extra_args)

    @staticmethod
    def formatter():
        def argparse_formatter(prog):
            return argparse.HelpFormatter(prog, max_help_position=50)

        return argparse_formatter
