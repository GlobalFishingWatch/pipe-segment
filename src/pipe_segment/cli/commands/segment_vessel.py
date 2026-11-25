"""CLI command that creates the segment-vessel table."""

from types import SimpleNamespace
from typing import Any

from gfw.common.cli import Command, Option

from pipe_segment.pipelines.segment_vessel import main
from pipe_segment.pipelines.segment_vessel.config import SegmentVesselConfig


DESCRIPTION = """\
Creates the segment-vessel table: a many-to-many mapping between seg_id, vessel_id and
ssvid, from the daily vessel_ids of the segments, over their whole history.

The table is rebuilt from scratch every time.

You can provide a configuration file or command-line arguments.
The latter take precedence, so if you provide both, command-line arguments
will overwrite options in the config file provided.
"""

HELP_VESSEL = "BigQuery table to read the daily vessel_ids of the segments from."
HELP_OUTPUT = "BigQuery table in which to store the segment-vessel mapping (truncated every run)."


class SegmentVessel(Command):
    """Creates the segment-vessel table."""

    @property
    def name(self) -> str:
        """The command's name."""
        return "segment-vessel"

    @property
    def description(self) -> str:
        """The command's description."""
        return DESCRIPTION

    @property
    def options(self) -> list[Option]:
        """The command's options."""
        return [
            Option("--bq-in-segment-vessel-daily", type=str, required=True, help=HELP_VESSEL),
            Option("--bq-out-segment-vessel", type=str, required=True, help=HELP_OUTPUT),
        ]

    @property
    def allow_unknown(self) -> bool:
        """Rejects unknown arguments: the command only runs queries, so it has no Beam options."""
        return False

    def run(self, config: SimpleNamespace, **kwargs: Any) -> Any:
        """Runs the pipeline with the resolved configuration."""
        return main.run(SegmentVesselConfig.from_namespace(config), **kwargs)
