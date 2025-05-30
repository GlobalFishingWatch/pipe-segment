import logging

from pipe_segment.segment_vessel import segment_vessel_daily
from pipe_segment.cli.commands.base import Command
from pipe_segment.cli.commands.validator import (
    valid_daterange, valid_frequency, valid_table_reference
)

logger = logging.getLogger(__name__)


class SegmentVesselDaily(Command):
    NAME = "segment_vessel_daily"
    HELP = "segment vessel daily pipeline."
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
        add(
            "--source_segment_identity", required=True, metavar='\b', type=valid_table_reference,
            help="Table, query or file to read segment identities from.")
        add(
            "--destination", required=True, metavar='\b', type=valid_table_reference,
            help="Table or file (prefix) to write daily segment vessel records.")
        add(
            "--date_range", metavar='\b', type=valid_daterange,
            help="Range of dates to read from source. Format: YYYY-MM-DD,YYYY-MM-DD.")
        add(
            "--labels", metavar='\b', action="append",
            help="The labels that are using to identify the jobs and audit them.")

        optional = p.add_argument_group("Optional")
        add = optional.add_argument
        add(
            "--window_days", type=int, metavar='\b', default=cls.DEFAULT_WINDOW_DAYS,
            help="Amount of days windowing the segment identities to identify a vessel_id. "
                 "(default: %(default)s).")
        add(
            "--single_ident_min_freq", type=valid_frequency, metavar='\b',
            default=cls.DEFAULT_ID_MIN_FREQ,
            help="Minimum threshold for segment identity frequency fields "
                 "that lets find a single dominant identity value. (default: %(default)s).")
        add(
            "--most_common_min_freq", type=valid_frequency, metavar='\b',
            default=cls.DEFAULT_MOST_COMMON_MIN_FREQ,
            help="The minimal frequency that defines a ssvid that have a single dominant identity."
                 " (default: %(default)s).")
        add(
            "--spoofing_threshold", type=int, metavar='\b', default=cls.DEFAULT_SPOOFING_THRESHOLD,
            help="The amount of spoofing points to consider a segment a noise one. "
                 "(default: %(default)s).")
        add(
            "--project", metavar='\b', default=cls.DEFAULT_PROJECT,
            help="The Google Cloud Project that will be billed to.")

    @classmethod
    def run(cls, args, extra_args):
        segment_vessel_daily.run(args, extra_args)
