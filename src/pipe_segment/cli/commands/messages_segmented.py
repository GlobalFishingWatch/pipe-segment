"""CLI command that segments the normalized messages."""

from types import SimpleNamespace
from typing import Any

from gfw.common.cli import Command, Option, date_range_options, wait_for_job_option

from pipe_segment.pipelines.messages_segmented import main
from pipe_segment.pipelines.messages_segmented.config import MessagesSegmentedConfig


DESCRIPTION = """\
Divides the tracks of the vessels into segments: contiguous pieces of track that come from
a single vessel, separating out the noise and the signals of two or more vessels that
broadcast with the same MMSI at the same time.

It writes the segmented messages, the fragments (pieces of track within a day) and the
segments-days, for the days in the range, after deleting the range's existing rows.
The range starts at start-date and ends before end-date.

By default, a run matches again the whole history of fragments before the date range.
With --incremental, it starts from the segments of the day before the date range instead.
It fails if a day of the range has no input messages, or if the segments table has no rows
for the day before while earlier fragments exist. On the first run of a pipeline, with no
earlier fragments, it starts from an empty state.

The satellite timing correction runs only if all of --bq-in-sat-offset-messages,
--bq-in-norad-to-receiver and --bq-in-sat-positions are given.

You can provide a configuration file or command-line arguments.
The latter take precedence, so if you provide both, command-line arguments
will overwrite options in the config file provided.

Besides the arguments defined here, you can also pass any pipeline option
defined for Apache Beam PipelineOptions class. For more information, see
    https://cloud.google.com/dataflow/docs/reference/pipeline-options#python.\n
"""

HELP_MESSAGES = "BigQuery table to read the normalized messages from."
HELP_OUT_MESSAGES = "BigQuery table in which to store the segmented messages."
HELP_OUT_SEGMENTS = "BigQuery table in which to store the segments-days."
HELP_OUT_FRAGMENTS = "BigQuery table in which to store the fragments."
HELP_IN_SEGMENTS = (
    "BigQuery table to read the segments of the day before the date range from, in "
    "incremental mode. The segments are joined with the fragments of --bq-in-fragments. "
    "Defaults to --bq-out-segments. It allows to read from production data and write to a "
    "scratch dataset."
)
HELP_IN_FRAGMENTS = (
    "BigQuery table to read the fragments from: the whole history before the date range, or "
    "in incremental mode the fragments of the day before it, which must be the ones that "
    "--bq-in-segments was built from. Defaults to --bq-out-fragments. It allows to read from "
    "production data and write to a scratch dataset."
)
HELP_SAT_OFFSET_MESSAGES = "BigQuery table to read the messages that the satellite offsets use."
HELP_NORAD = "BigQuery table that links NORAD IDs and receivers."
HELP_SAT_POSITIONS = "BigQuery table with the distance to the satellite by receiver at 1s."
HELP_OUT_SAT_OFFSETS = "BigQuery table in which to store the satellite offsets."
HELP_BAD_HOUR = "Hours on either side of an hour with bad satellite timing to suppress."
HELP_MAX_OFFSET = "Maximum offset (in seconds) of a satellite clock before its messages drop."
HELP_BINS = "Amount of containers per day to tag fragments and messages."
HELP_SSVID_FILTER = (
    "Query that returns a list of ssvid to trim the sourced data down to. Note that the "
    "returned list is used in memory so should not be too large. This is meant for testing "
    "purposes. If tempted to use it for production, more work should be done so that the data "
    "is pruned on the way in."
)
HELP_INCREMENTAL = "Start from the segments of the day before the date range."


class MessagesSegmented(Command):
    """Segments the normalized messages."""

    @property
    def name(self) -> str:
        """The command's name."""
        return "messages-segmented"

    @property
    def description(self) -> str:
        """The command's description."""
        return DESCRIPTION

    @property
    def options(self) -> list[Option]:
        """The command's options."""
        return [
            Option("--bq-in-normalized-messages", type=str, required=True, help=HELP_MESSAGES),
            Option("--bq-out-segmented-messages", type=str, required=True, help=HELP_OUT_MESSAGES),
            Option("--bq-out-segments", type=str, required=True, help=HELP_OUT_SEGMENTS),
            Option("--bq-out-fragments", type=str, required=True, help=HELP_OUT_FRAGMENTS),
            Option("--bq-in-segments", type=str, help=HELP_IN_SEGMENTS),
            Option("--bq-in-fragments", type=str, help=HELP_IN_FRAGMENTS),
            Option("--bq-in-sat-offset-messages", type=str, help=HELP_SAT_OFFSET_MESSAGES),
            Option("--bq-in-norad-to-receiver", type=str, help=HELP_NORAD),
            Option("--bq-in-sat-positions", type=str, help=HELP_SAT_POSITIONS),
            Option("--bq-out-sat-offsets", type=str, help=HELP_OUT_SAT_OFFSETS),
            *date_range_options(),
            Option("--bad-hour-padding", type=int, default=1, help=HELP_BAD_HOUR),
            Option("--max-timing-offset-s", type=int, default=30, help=HELP_MAX_OFFSET),
            Option("--bins-per-day", type=int, default=4, help=HELP_BINS),
            Option("--ssvid-filter-query", type=str, help=HELP_SSVID_FILTER),
            Option("--incremental", type=bool, default=False, help=HELP_INCREMENTAL),
            wait_for_job_option(),
        ]

    def run(self, config: SimpleNamespace, **kwargs: Any) -> Any:
        """Runs the pipeline with the resolved configuration."""
        return main.run(MessagesSegmentedConfig.from_namespace(config), **kwargs)
