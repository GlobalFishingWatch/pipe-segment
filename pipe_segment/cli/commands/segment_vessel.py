import logging

from pipe_segment.segment_vessel import segment_vessel
from pipe_segment.cli.commands.base import Command
from pipe_segment.cli.commands.validator import (
    valid_daterange, valid_frequency, valid_table_shortpath
)

logger = logging.getLogger(__name__)


class SegmentVessel(Command):
    NAME = "segment_vessel"

    HELP = "segment vessel pipeline."
    HELP_SOURCE_SEGMENT_VESSEL_DAILY = "Table, query or file to read segment vessel daily from."
    HELP_DEST = "Table to write segment vessel records."
    HELP_LABELS = "The labels that are using to identify the jobs and audit them."
    HELP_PROJECT = "The Google Cloud Project."
    EPILOG = "Example: pipe segment_vessel --help"

    DEFAULT_PROJECT = "world-fishing-827"

    @classmethod
    def add_to_subparsers(cls, subparsers):
        p = subparsers.add_parser(
            cls.NAME, help=cls.HELP, epilog=cls.EPILOG, formatter_class=cls.formatter())

        p.set_defaults(func=cls.run)

        required = p.add_argument_group("Required")
        add = required.add_argument
        add("--source_segment_vessel_daily", required=True, metavar='\b',
            type=valid_table_shortpath, help=cls.HELP_SOURCE_SEGMENT_VESSEL_DAILY)
        add("--destination", required=True, metavar='\b',
            type=valid_table_shortpath, help=cls.HELP_DEST)
        add("--labels", metavar='\b', action="append", help=cls.HELP_LABELS)

        optional = p.add_argument_group("Optional")
        add = optional.add_argument
        add("--project", metavar='\b', default=cls.DEFAULT_PROJECT, help=cls.HELP_PROJECT)

    @classmethod
    def run(cls, args, extra_args):
        segment_vessel.run(args, extra_args)
