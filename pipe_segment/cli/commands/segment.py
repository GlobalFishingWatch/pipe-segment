import logging

from pipe_segment import pipeline
from pipe_segment.cli.commands.base import Command
from pipe_segment.cli.commands.validator import (
    valid_daterange,
    valid_table_reference,
)

logger = logging.getLogger(__name__)


class Segment(Command):
    NAME = "segment"
    HELP = "segment pipeline."
    EPILOG = "Example: pipe segment --help"

    @classmethod
    def add_to_subparsers(cls, subparsers):
        p = subparsers.add_parser(
            cls.NAME, help=cls.HELP, epilog=cls.EPILOG, formatter_class=cls.formatter())

        p.set_defaults(func=cls.run)

        required = p.add_argument_group("Required")
        add = required.add_argument
        add(
            "--in_normalized_messages_table", required=True, metavar='\b',
            type=valid_table_reference,
            help="Table to read normalized messages.")
        add(
            "--out_segmented_messages_table", required=True, metavar='\b',
            type=valid_table_reference,
            help="Table to write segmented messages.")
        add(
            "--fragments_table", required=True, metavar='\b', type=valid_table_reference,
            help="Table to read and write fragments.")
        add(
            "--out_segments_table", required=True, metavar='\b', type=valid_table_reference,
            help="Table to write segments-days.")

        optional = p.add_argument_group("Optional")
        add = optional.add_argument
        add(
            "--in_normalized_sat_offset_messages_table", metavar=' ', type=valid_table_reference,
            help="Table, query or file to read normalized messages. Subset of `source`.")
        add(
            "--in_norad_to_receiver_table", metavar=' ', type=valid_table_reference,
            help="Table that links NORAD IDs and receivers.")
        add(
            "--in_sat_positions_table", metavar=' ', type=valid_table_reference,
            help="Table with distance to satellite by receiver at 1s resolution.")
        add(
            "--out_sat_offsets_table", metavar=' ', type=valid_table_reference,
            help="Table to write satellite offsets to.")
        add(
            "--bad_hour_padding", type=int, default=1, metavar=' ',
            help="Hours on either side of an hour with bad satellite timing to suppress.")
        add(
            "--max_timing_offset_s", type=int, default=30, metavar=' ',
            help="Max. offset (in seconds) of a satellite clock before we drop its messages.")
        add(
            "--date_range", metavar=' ', type=valid_daterange,
            help="Range of dates to read from source. Format 'YYYY-MM-DD,YYYY-MM-DD'.")
        add(
            "--wait_for_job", action="store_true",
            help="Wait until the job finishes before returning.")
        add(
            "--ssvid_filter_query", metavar=' ',
            help=("Query that returns a list of ssvid to trim the sourced data down to. "
                 "Note that the returned list is used in memory so should not be too large. "
                 "This meant for testing purposes. If tempted to use for production, "
                 "more work should be done so that the data is pruned on the way in."))
        add(
            "--bins_per_day", default=4, metavar=' ', type=int,
            help="Amount of containers per day to tag fragments and messages.")
        add(
            "--out_fragments_table", default=None, metavar=' ', type=valid_table_reference,
            help=("Output table for fragments. This parameter allows to specify a different "
                 "table from the one you use to read fragments from, which is useful on "
                 "testing scenarios (for example, to read from production data and "
                 "write to a scratch dataset)"))

    @classmethod
    def run(cls, args, extra_args):
        pipeline.run(args, extra_args)
