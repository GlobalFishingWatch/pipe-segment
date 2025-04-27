import logging

from pipe_segment.vessel_info import vessel_info
from pipe_segment.cli.commands.base import Command
from pipe_segment.cli.commands.validator import (
    valid_daterange, valid_frequency, valid_table_shortpath
)

logger = logging.getLogger(__name__)


class VesselInfo(Command):
    NAME = "vessel_info"

    HELP = "vessel info pipeline."
    HELP_SOURCE_SEGMENT_IDENTITY = "Table, query or file to read segment identities daily from."
    HELP_SOURCE_SEGMENT_VESSEL = "Table, query or file to read segment vessel daily from."
    HELP_DEST = "Table to write vessel info records."
    HELP_LABELS = "The labels that are using to identify the jobs and audit them."
    HELP_MOST_COMMON_MIN_FREQ = (
        "The minimal frequency that defines a ssvid that have a single dominant identity. "
        "(default: %(default)s)."
    )
    HELP_PROJECT = "The Google Cloud Project."
    EPILOG = "Example: pipe vessel_info --help"

    DEFAULT_MOST_COMMON_MIN_FREQ = 0.05
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
        add("--source_segment_vessel", required=True, metavar='\b',
            type=valid_table_shortpath, help=cls.HELP_SOURCE_SEGMENT_VESSEL)
        add("--destination", required=True, metavar='\b',
            type=valid_table_shortpath, help=cls.HELP_DEST)
        add("--labels", metavar='\b', action="append", help=cls.HELP_LABELS)

        optional = p.add_argument_group("Optional")
        add = optional.add_argument
        add("--most_common_min_freq", type=valid_frequency, metavar='\b',
            default=cls.DEFAULT_MOST_COMMON_MIN_FREQ, help=cls.HELP_MOST_COMMON_MIN_FREQ)
        add("--project", metavar='\b', default=cls.DEFAULT_PROJECT, help=cls.HELP_PROJECT)

    @classmethod
    def run(cls, args, extra_args):
        vessel_info.run(args, extra_args)
