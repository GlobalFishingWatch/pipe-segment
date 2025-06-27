import logging

from pipe_segment.segment_identity import pipeline
from pipe_segment.cli.commands.base import Command
from pipe_segment.cli.commands.validator import (
    valid_daterange,
    valid_table_reference,
    valid_positive_int,
)

logger = logging.getLogger(__name__)


class SegmentIdentity(Command):
    NAME = "segment_identity"
    HELP = "segment identity pipeline."
    EPILOG = "Example: pipe segment_identity --help"

    @classmethod
    def add_to_subparsers(cls, subparsers):
        p = subparsers.add_parser(
            cls.NAME, help=cls.HELP, epilog=cls.EPILOG, formatter_class=cls.formatter())

        p.set_defaults(func=cls.run)

        required = p.add_argument_group("Required")
        add = required.add_argument
        add(
            "--source_segments", required=True, metavar='\b', type=valid_table_reference,
            help="Table, query or file to read segments from.")
        add(
            "--source_fragments", required=True, metavar='\b', type=valid_table_reference,
            help="Table, query or file to read fragments from.")
        add(
            "--dest_segment_identity", required=True, metavar='\b', type=valid_table_reference,
            help="Table to write daily segment identity records.")

        optional = p.add_argument_group("Optional")
        add = optional.add_argument
        add(
            "--date_range", metavar='\b', type=valid_daterange,
            help="Range of dates to read from source. Format: YYYY-MM-DD,YYYY-MM-DD.")
        add(
            "--wait_for_job", action="store_true",
            help="Wait until the job finishes before returning.")
        add(
            "--temp_shards_per_day", type=valid_positive_int, metavar='\b', default=16,
            help="Number of shards to write per day in output temporary storage. "
                 "A good value for this is the max number of workers. (default: %(default)s).")

    @classmethod
    def run(cls, args, extra_args):
        pipeline.run(args, extra_args)
