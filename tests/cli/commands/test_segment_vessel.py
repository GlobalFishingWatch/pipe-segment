"""Tests for the segment-vessel CLI command."""

import argparse

from pathlib import Path
from unittest.mock import MagicMock

import pytest

from pytest_mock import MockerFixture

from pipe_segment.cli import main
from pipe_segment.pipelines.segment_vessel.config import SegmentVesselConfig


BASE_ARGS = [
    "segment-vessel",
    "--bq-in-segment-vessel-daily",
    "project.dataset.segment_vessel_daily",
    "--bq-out-segment-vessel",
    "project.dataset.segment_vessel",
    "--project",
    "test-project",
    "--labels",
    "environment=test",
]


@pytest.fixture
def run(mocker: MockerFixture) -> MagicMock:
    """Mocks the pipeline run that the CLI calls."""
    return mocker.patch("pipe_segment.cli.commands.segment_vessel.main.run")


def test_cli_executes_run_with_the_config(run: MagicMock) -> None:
    """Checks that the CLI builds the config and passes it to run."""
    main.run(BASE_ARGS)

    config = run.call_args.args[0]
    assert isinstance(config, SegmentVesselConfig)
    assert config.bq_in_segment_vessel_daily == "project.dataset.segment_vessel_daily"
    assert config.bq_out_segment_vessel == "project.dataset.segment_vessel"
    assert config.project == "test-project"
    assert config.labels == {"environment": "test"}


@pytest.mark.parametrize(
    "option",
    ["--bq-in-segment-vessel-daily", "--bq-out-segment-vessel", "--project", "--labels"],
)
def test_cli_requires_the_required_options(run: MagicMock, option: str) -> None:
    """Checks that the CLI rejects a missing required option."""
    index = BASE_ARGS.index(option)
    args = [*BASE_ARGS[:index], *BASE_ARGS[index + 2:]]

    with pytest.raises(argparse.ArgumentTypeError, match=option.lstrip("-").replace("-", "_")):
        main.run(args)

    run.assert_not_called()


def test_cli_reads_the_options_from_a_config_file(run: MagicMock, tmp_path: Path) -> None:
    """Checks that the options can come from a config file."""
    config_file = tmp_path / "config.yaml"
    config_file.write_text(
        "bq_in_segment_vessel_daily: p.d.vessel\n"
        "bq_out_segment_vessel: p.d.out\n"
        "project: file-project\n"
        "labels:\n"
        "  environment: file\n"
    )

    main.run(["segment-vessel", "-c", str(config_file)])

    config = run.call_args.args[0]
    assert config.bq_in_segment_vessel_daily == "p.d.vessel"
    assert config.bq_out_segment_vessel == "p.d.out"
    assert config.project == "file-project"
    assert config.labels == {"environment": "file"}
