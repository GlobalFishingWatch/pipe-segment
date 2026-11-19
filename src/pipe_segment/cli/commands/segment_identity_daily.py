"""CLI command that creates the daily identity of the segments."""

from types import SimpleNamespace
from typing import Any

from gfw.common.cli import Command, Option, date_range_options

from pipe_segment.pipelines.segment_identity_daily import main
from pipe_segment.pipelines.segment_identity_daily.config import SegmentIdentityDailyConfig


DESCRIPTION = """\
Summarizes the identity of each segment, per day: the names, callsigns and IMO numbers that
its messages broadcast, with the number of times of each one.

The days in the range are read from the segments and the fragments, and each one is written
to its partition of the output table, after deleting the range's existing rows.
The range starts at start-date and ends before end-date.

Besides the arguments defined here, you can also pass any pipeline option
defined for Apache Beam PipelineOptions class. For more information, see
    https://cloud.google.com/dataflow/docs/reference/pipeline-options#python.\n
"""

HELP_SEGMENTS = "BigQuery table to read the segments from."
HELP_FRAGMENTS = "BigQuery table to read the fragments from."
HELP_OUTPUT = "BigQuery table in which to store the daily identities of the segments."
HELP_WAIT = "Wait until the job finishes before returning."


class SegmentIdentityDaily(Command):
    """Creates the daily identity of the segments."""

    @property
    def name(self) -> str:
        """The command's name."""
        return "segment-identity-daily"

    @property
    def description(self) -> str:
        """The command's description."""
        return DESCRIPTION

    @property
    def options(self) -> list[Option]:
        """The command's options."""
        return [
            Option("--bq-in-segments", type=str, required=True, help=HELP_SEGMENTS),
            Option("--bq-in-fragments", type=str, required=True, help=HELP_FRAGMENTS),
            Option("--bq-out-segment-identity-daily", type=str, required=True, help=HELP_OUTPUT),
            *date_range_options(),
            Option("--wait-for-job", type=bool, default=False, help=HELP_WAIT),
        ]

    def run(self, config: SimpleNamespace, **kwargs: Any) -> Any:
        """Runs the pipeline with the resolved configuration."""
        return main.run(SegmentIdentityDailyConfig.from_namespace(config), **kwargs)
