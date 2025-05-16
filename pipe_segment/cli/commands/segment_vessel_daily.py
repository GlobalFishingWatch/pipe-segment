import logging

from pipe_segment.segment_vessel import segment_vessel_daily
from pipe_segment.cli.commands.base import Command
from pipe_segment.cli.commands.validator import (
    valid_daterange, valid_frequency, valid_table_shortpath
)

logger = logging.getLogger(__name__)


class SegmentVesselDaily(Command):
    NAME = "segment_vessel_daily"

    HELP = "segment vessel daily pipeline."
    HELP_SOURCE_SEGMENT_IDENTITY = "Table, query or file to read segment identities from."
    HELP_DEST = "Table or file (prefix) to write daily segment vessel records."

    HELP_DATE_RANGE = "Range of dates to read from source. Format: YYYY-MM-DD,YYYY-MM-DD."
    HELP_LABELS = "The labels that are using to identify the jobs and audit them."
    HELP_WINDOW_DAYS = (
        "Amount of days windowing the segment identities to identify a vessel_id. "
        "(default: %(default)s)."
    )
    HELP_ID_MIN_FREQ = (
        "Minimum threshold for segment identity frequency fields "
        "that lets find a single dominant identity value. "
        " (default: %(default)s)."
    )
    HELP_MOST_COMMON_MIN_FREQ = (
        "The minimal frequency that defines a ssvid that have a single dominant identity. "
        "(default: %(default)s)."
    )
    HELP_SPOOFING = (
        "The amount of spoofing points to consider a segment a noise one. "
        "(default: %(default)s)."
    )
    HELP_PROJECT = "The Google Cloud Project."
    EPILOG = "Example: pipe segment_vessel_daily --help"

    DEFAULT_WINDOW_DAYS = 30
    DEFAULT_ID_MIN_FREQ = 0.99
    DEFAULT_MOST_COMMON_MIN_FREQ = 0.05
    DEFAULT_SPOOFING_THRESHOLD = 10
    DEFAULT_PROJECT = "world-fishing-827"

    @classmethod
    def add_to_subparsers(cls, subparsers):
        p = subparsers.add_parser(
            cls.NAME, help=cls.HELP, epilog=cls.EPILOG, formatter_class=cls.formatter())

        p.set_defaults(func=cls.run)

        required = p.add_argument_group("Required")
        add = required.add_argument
        add("--source_segment_identity", required=True, metavar='\b',
            type=valid_table_shortpath, help=cls.HELP_SOURCE_SEGMENT_IDENTITY)
        add("--destination", required=True, metavar='\b',
            type=valid_table_shortpath, help=cls.HELP_DEST)
        add("--date_range", metavar='\b', type=valid_daterange, help=cls.HELP_DATE_RANGE)
        add("--labels", metavar='\b', action="append", help=cls.HELP_LABELS)

        optional = p.add_argument_group("Optional")
        add = optional.add_argument
        add("--window_days", type=int, metavar='\b',
            default=cls.DEFAULT_WINDOW_DAYS, help=cls.HELP_WINDOW_DAYS)
        add("--single_ident_min_freq", type=valid_frequency, metavar='\b',
            default=cls.DEFAULT_ID_MIN_FREQ, help=cls.HELP_ID_MIN_FREQ)
        add("--most_common_min_freq", type=valid_frequency, metavar='\b',
            default=cls.DEFAULT_MOST_COMMON_MIN_FREQ, help=cls.HELP_MOST_COMMON_MIN_FREQ)
        add("--spoofing_threshold", type=int, metavar='\b',
            default=cls.DEFAULT_SPOOFING_THRESHOLD, help=cls.HELP_SPOOFING)
        add("--project", metavar='\b', default=cls.DEFAULT_PROJECT, help=cls.HELP_PROJECT)

    @classmethod
    def run(cls, args, extra_args):
        segment_vessel_daily.run(args, extra_args)
