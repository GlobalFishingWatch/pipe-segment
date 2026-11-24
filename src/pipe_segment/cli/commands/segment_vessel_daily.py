"""CLI command that creates the daily segment-vessel table."""

from types import SimpleNamespace
from typing import Any

from gfw.common.cli import Command, Option, date_range_options

from pipe_segment.pipelines.segment_vessel_daily import main
from pipe_segment.pipelines.segment_vessel_daily.config import SegmentVesselDailyConfig


DESCRIPTION = """\
Assigns a vessel_id to each segment, per day: the identities of the segments over a window of
days are summarized to find the vessel that each segment belongs to.

The days in the range are processed one by one, and each one is written to its partition
of the output table, after deleting the range's existing rows.
The range starts at start-date and ends before end-date.

You can provide a configuration file or command-line arguments.
The latter take precedence, so if you provide both, command-line arguments
will overwrite options in the config file provided.
"""

HELP_IDENTITY = "BigQuery table to read the daily identities of the segments from."
HELP_OUTPUT = "BigQuery table in which to store the daily vessel_ids of the segments."
HELP_WINDOW = "Days of segment identities that are windowed to identify a vessel_id."
HELP_SINGLE_IDENT = (
    "Minimum frequency (between 0 and 1) of an identity value to be the single dominant one "
    "of a segment."
)
HELP_MIN_FREQ = (
    "Minimal frequency (between 0 and 1) that defines a ssvid that has a single dominant "
    "identity."
)
HELP_DRY_RUN = "If True, only executes the queries of the days, in dry run mode."
HELP_SPOOFING = "Amount of spoofing points from which a segment is considered noise."


class SegmentVesselDaily(Command):
    """Creates the daily segment-vessel table."""

    @property
    def name(self) -> str:
        """The command's name."""
        return "segment-vessel-daily"

    @property
    def description(self) -> str:
        """The command's description."""
        return DESCRIPTION

    @property
    def options(self) -> list[Option]:
        """The command's options."""
        return [
            Option("--bq-in-segment-identity-daily", type=str, required=True, help=HELP_IDENTITY),
            Option("--bq-out-segment-vessel-daily", type=str, required=True, help=HELP_OUTPUT),
            *date_range_options(),
            Option("--window-days", type=int, default=30, help=HELP_WINDOW),
            Option("--single-ident-min-freq", type=float, default=0.99, help=HELP_SINGLE_IDENT),
            Option("--most-common-min-freq", type=float, default=0.05, help=HELP_MIN_FREQ),
            Option("--spoofing-threshold", type=int, default=10, help=HELP_SPOOFING),
            Option("--dry-run", type=bool, help=HELP_DRY_RUN),
        ]

    @property
    def allow_unknown(self) -> bool:
        """Rejects unknown arguments: the command only runs queries, so it has no Beam options."""
        return False

    def run(self, config: SimpleNamespace, **kwargs: Any) -> Any:
        """Runs the pipeline with the resolved configuration."""
        return main.run(SegmentVesselDailyConfig.from_namespace(config), **kwargs)
