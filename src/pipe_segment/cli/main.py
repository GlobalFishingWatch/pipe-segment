"""Command-line interface of pipe-segment, built on gfw-common's CLI."""

import sys

from typing import Any

from gfw.common.cli import CLI, labels_option
from gfw.common.cli.formatting import default_formatter
from gfw.common.logging import LoggerConfig

from pipe_segment.cli.commands import (
    MessagesSegmented,
    SegmentIdentityDaily,
    SegmentInfo,
    SegmentVessel,
    SegmentVesselDaily,
    VesselInfo,
)
from pipe_segment.cli.options import project_option
from pipe_segment.version import __version__


NAME = "pipe-segment"
DESCRIPTION = (
    "Divides vessel tracks into contiguous segments, separating out signals that come "
    "from two or more vessels which are broadcasting using the same MMSI at the same time."
)
EXAMPLES = (
    "pipe-segment segment "
    "--bq-in-normalized-messages project.dataset.messages_normalized "
    "--bq-out-segmented-messages project.dataset.messages_segmented "
    "--bq-out-segments project.dataset.segments "
    "--bq-out-fragments project.dataset.fragments "
    "--start-date 2025-01-01 --end-date 2025-01-02 "
    "--project my-project "
    "--labels environment=development resource_creator=tomas-link",
    "pipe-segment messages-segmented -c config/messages-segmented/example.yaml",
    "pipe-segment messages-segmented -c config/messages-segmented/incremental.yaml",
    "pipe-segment vessel-info "
    "--bq-in-segment-identity-daily project.dataset.segment_identity_daily "
    "--bq-in-segment-vessel-daily project.dataset.segment_vessel_daily "
    "--bq-out-vessel-info project.dataset.vessel_info "
    "--project my-project "
    "--labels environment=development resource_creator=tomas-link",
    "pipe-segment vessel-info -c config/vessel-info/example.yaml",
    "pipe-segment segment-info "
    "--bq-in-segment-identity-daily project.dataset.segment_identity_daily "
    "--bq-in-segment-vessel-daily project.dataset.segment_vessel_daily "
    "--bq-out-segment-info project.dataset.segment_info "
    "--project my-project "
    "--labels environment=development resource_creator=tomas-link",
    "pipe-segment segment-info -c config/segment-info/example.yaml",
    "pipe-segment segment-vessel "
    "--bq-in-segment-vessel-daily project.dataset.segment_vessel_daily "
    "--bq-out-segment-vessel project.dataset.segment_vessel "
    "--project my-project "
    "--labels environment=development resource_creator=tomas-link",
    "pipe-segment segment-vessel -c config/segment-vessel/example.yaml",
    "pipe-segment segment-vessel-daily "
    "--bq-in-segment-identity-daily project.dataset.segment_identity_daily "
    "--bq-out-segment-vessel-daily project.dataset.segment_vessel_daily "
    "--start-date 2025-01-01 --end-date 2025-01-02 "
    "--project my-project "
    "--labels environment=development resource_creator=tomas-link",
    "pipe-segment segment-vessel-daily -c config/segment-vessel-daily/example.yaml",
    "pipe-segment segment-identity-daily "
    "--bq-in-segments project.dataset.segments "
    "--bq-in-fragments project.dataset.fragments "
    "--bq-out-segment-identity-daily project.dataset.segment_identity_daily "
    "--start-date 2025-01-01 --end-date 2025-01-02 "
    "--project my-project "
    "--labels environment=development resource_creator=tomas-link",
    "pipe-segment segment-identity-daily -c config/segment-identity-daily/example.yaml",
)

COMMANDS = [
    MessagesSegmented,
    SegmentIdentityDaily,
    SegmentVesselDaily,
    SegmentVessel,
    SegmentInfo,
    VesselInfo,
]


def run(args: list[str]) -> Any:
    """Executes the command given in ``args``.

    Args:
        args:
            Command-line arguments, without the program name.

    Returns:
        The result of the command.
    """
    cli = CLI(
        name=NAME,
        description=DESCRIPTION,
        formatter=default_formatter(max_pos=120),
        subcommands=COMMANDS,
        options=[labels_option(), project_option()],  # Common options for all subcommands.
        version=__version__,
        examples=EXAMPLES,
        logger_config=LoggerConfig(
            warning_level=[
                "apache_beam.runners.portability",
                "apache_beam.runners.worker",
                "apache_beam.transforms.core",
                "apache_beam.io.filesystem",
                "apache_beam.io.gcp.bigquery_tools",
                "urllib3",
            ]
        ),
        allow_unknown=True,
    )

    result, _ = cli.execute(args)
    return result


def main() -> None:
    """Runs the CLI with the arguments of the command line.

    A job that does not finish in DONE raises, so that the process (and the Airflow task that
    runs it) fails.
    """
    run(sys.argv[1:])


if __name__ == "__main__":
    main()
