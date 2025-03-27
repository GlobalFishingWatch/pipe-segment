import json
import logging

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

    HELP = "segment pipeline."
    HELP_IN_NORMALIZED_MESSAGES_TABLE = "Table to read normalized messages."
    HELP_OUT_SEGMENTED_MESSAGES_TABLE = "Table to write segmented messages."
    HELP_FRAGMENTS_TABLE = "Table to read and write fragments."
    HELP_OUT_SEGMENTS_TABLE = "Table to write segments-days."
    HELP_IN_NORMALIZED_SAT_OFFSET_MESSAGES_TABLE = "Table, query or file to read normalized messages. Subset of `source`."
    HELP_IN_NORAD_TO_RECEIVER_TABLE = "Table that links NORAD IDs and receivers."
    HELP_IN_SAT_POSITIONS_TABLE = "Table with distance to satellite by receiver at 1s resolution."
    HELP_OUT_SAT_OFFSETS_TABLE = "Table to write satellite offsets to."
    HELP_BAD_HOUR_PADDING = "Hours on either side of an hour with bad satellite timing to suppress."
    HELP_MAX_TIMING_OFFSET_S = "Max. offset (in seconds) of a satellite clock before we drop its messages."
    HELP_DATE_RANGE = "Range of dates to read from source. Format 'YYYY-MM-DD,YYYY-MM-DD'."
    HELP_WAIT_FOR_JOB = "Wait until the job finishes before returning."

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

    HELP_OUT_FRAGMENTS_TABLE = """Output table for fragments. This parameter
        allows to specify a different table from the one you use to read fragments
        from, which is useful on testing scenarios (for example, to read from
        production data and write to a scratch dataset)"""

    EPILOG = "Example: pipe segment --help"

    @classmethod
    def add_to_subparsers(cls, subparsers):
        p = subparsers.add_parser(
            cls.NAME, help=cls.HELP, epilog=cls.EPILOG, formatter_class=cls.formatter())

        p.set_defaults(func=cls.run)

        required = p.add_argument_group("Required")
        add = required.add_argument
        add("--in_normalized_messages_table", required=True, metavar='\b', help=cls.HELP_IN_NORMALIZED_MESSAGES_TABLE)
        add("--out_segmented_messages_table", required=True, metavar='\b', help=cls.HELP_OUT_SEGMENTED_MESSAGES_TABLE)
        add("--fragments_table", required=True, metavar='\b', help=cls.HELP_FRAGMENTS_TABLE)
        add("--out_segments_table", required=True, metavar='\b', help=cls.HELP_OUT_SEGMENTS_TABLE)

        optional = p.add_argument_group("Optional")
        add = optional.add_argument
        add("--in_normalized_sat_offset_messages_table", metavar=' ', help=cls.HELP_IN_NORMALIZED_SAT_OFFSET_MESSAGES_TABLE)
        add("--in_norad_to_receiver_table", metavar=' ', help=cls.HELP_IN_NORAD_TO_RECEIVER_TABLE)
        add("--in_sat_positions_table", metavar=' ', help=cls.HELP_IN_SAT_POSITIONS_TABLE)
        add("--out_sat_offsets_table", metavar=' ', help=cls.HELP_OUT_SAT_OFFSETS_TABLE)
        add("--bad_hour_padding", type=int, default=1, metavar=' ', help=cls.HELP_BAD_HOUR_PADDING)
        add("--max_timing_offset_s", type=int, default=30, metavar=' ', help=cls.HELP_MAX_TIMING_OFFSET_S)
        add("--date_range", metavar=' ', help=cls.HELP_DATE_RANGE)
        add("--wait_for_job", action="store_true", help=cls.HELP_WAIT_FOR_JOB)
        add("--segmenter_params", default="{}", metavar=' ', help=cls.HELP_SEGMENTER_PARAMS)
        add("--merge_params", default="{}", metavar=' ', help=cls.HELP_MERGE_PARAMS)
        add("--ssvid_filter_query", metavar=' ', help=cls.HELP_SSVID_FILTER)
        add("--bins_per_day", default=4, metavar=' ', help=cls.HELP_BINS_PER_DAY)
        add("--out_fragments_table", default=None, metavar=' ', help=cls.HELP_OUT_FRAGMENTS_TABLE)

    @classmethod
    def run(cls, args, extra_args):
        pipeline.run(args, extra_args)
