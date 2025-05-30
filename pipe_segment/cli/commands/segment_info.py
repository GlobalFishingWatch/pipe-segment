import logging

from pipe_segment.segment_info import segment_info
from pipe_segment.cli.commands.base import Command
from pipe_segment.cli.commands.validator import (
    valid_frequency, valid_table_reference
)

logger = logging.getLogger(__name__)


class SegmentInfo(Command):
    NAME = "segment_info"
    HELP = "segment info pipeline."
    EPILOG = "Example: pipe segment_info --help"
    DEFAULT_MOST_COMMON_MIN_FREQ = 0.05
    DEFAULT_PROJECT = "world-fishing-827"

    @classmethod
    def add_to_subparsers(cls, subparsers):
        p = subparsers.add_parser(
            cls.NAME, help=cls.HELP, epilog=cls.EPILOG, formatter_class=cls.formatter())

        p.set_defaults(func=cls.run)

        required = p.add_argument_group("Required")
        add = required.add_argument
        add(
            "--source_segment_identity", required=True, metavar='\b',
            type=valid_table_reference,
            help="Table, query or file to read segment identities daily from.")
        add(
            "--source_segment_vessel", required=True, metavar='\b',
            type=valid_table_reference,
            help="Table, query or file to read segment vessel daily from.")
        add(
            "--destination", required=True, metavar='\b',
            type=valid_table_reference,
            help="Table to write segment info records.")
        add(
            "--labels", metavar='\b', action="append",
            help="The labels that are using to identify the jobs and audit them.")

        optional = p.add_argument_group("Optional")
        add = optional.add_argument
        add(
            "--most_common_min_freq", type=valid_frequency, metavar='\b',
            default=cls.DEFAULT_MOST_COMMON_MIN_FREQ,
            help="The minimal frequency that defines a ssvid that have a single dominant identity."
                 " (default: %(default)s).")
        add(
            "--project", metavar='\b', default=cls.DEFAULT_PROJECT,
            help="The Google Cloud Project that will be billed to.")

    @classmethod
    def run(cls, args, extra_args):
        segment_info.run(args, extra_args)
