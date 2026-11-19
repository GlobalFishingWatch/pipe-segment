"""CLI command that creates the vessel info table."""

from types import SimpleNamespace
from typing import Any

from gfw.common.cli import Command, Option

from pipe_segment.pipelines.vessel_info import main
from pipe_segment.pipelines.vessel_info.config import VesselInfoConfig


DESCRIPTION = """\
Creates the vessel info table, with one row per vessel_id: the identity of the vessel
summarized over its whole history, from the daily identities of its segments and the
segments that each vessel_id has had.

The table is rebuilt from scratch every time.

You can provide a configuration file or command-line arguments.
The latter take precedence, so if you provide both, command-line arguments
will overwrite options in the config file provided.
"""

HELP_IDENTITY = "BigQuery table to read the daily identities of the segments from."
HELP_VESSEL = "BigQuery table to read the daily vessel_ids of the segments from."
HELP_OUTPUT = "BigQuery table in which to store the vessel info (truncated on every run)."
HELP_MIN_FREQ = (
    "Minimal frequency (between 0 and 1) that defines a ssvid that has a single dominant "
    "identity."
)


class VesselInfo(Command):
    """Creates the vessel info table."""

    @property
    def name(self) -> str:
        """The command's name."""
        return "vessel-info"

    @property
    def description(self) -> str:
        """The command's description."""
        return DESCRIPTION

    @property
    def options(self) -> list[Option]:
        """The command's options."""
        return [
            Option("--bq-in-segment-identity-daily", type=str, required=True, help=HELP_IDENTITY),
            Option("--bq-in-segment-vessel-daily", type=str, required=True, help=HELP_VESSEL),
            Option("--bq-out-vessel-info", type=str, required=True, help=HELP_OUTPUT),
            Option("--most-common-min-freq", type=float, default=0.05, help=HELP_MIN_FREQ),
        ]

    def run(self, config: SimpleNamespace, **kwargs: Any) -> Any:
        """Runs the pipeline with the resolved configuration."""
        return main.run(VesselInfoConfig.from_namespace(config), **kwargs)
