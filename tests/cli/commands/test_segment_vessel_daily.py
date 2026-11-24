"""Tests for the segment-vessel-daily CLI command."""

import argparse

from datetime import date
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from pytest_mock import MockerFixture

from gfw.common.config import PipelineConfigError
from pipe_segment.cli import main
from pipe_segment.pipelines.segment_vessel_daily.config import SegmentVesselDailyConfig


BASE_ARGS = [
    "segment-vessel-daily",
    "--bq-in-segment-identity-daily",
    "project.dataset.segment_identity_daily",
    "--bq-out-segment-vessel-daily",
    "project.dataset.segment_vessel_daily",
    "--start-date",
    "2024-01-01",
    "--end-date",
    "2024-01-08",
    "--project",
    "test-project",
    "--labels",
    "environment=test",
]


@pytest.fixture
def run(mocker: MockerFixture) -> MagicMock:
    """Mocks the pipeline run that the CLI calls."""
    return mocker.patch("pipe_segment.cli.commands.segment_vessel_daily.main.run")


def test_cli_executes_run_with_the_config(run: MagicMock) -> None:
    """Checks that the CLI builds the config and passes it to run."""
    main.run(BASE_ARGS)

    config = run.call_args.args[0]
    assert isinstance(config, SegmentVesselDailyConfig)
    assert config.bq_in_segment_identity_daily == "project.dataset.segment_identity_daily"
    assert config.bq_out_segment_vessel_daily == "project.dataset.segment_vessel_daily"
    assert config.start_date == date(2024, 1, 1)
    assert config.end_date == date(2024, 1, 8)
    assert config.window_days == 30
    assert config.single_ident_min_freq == 0.99
    assert config.most_common_min_freq == 0.05
    assert config.spoofing_threshold == 10
    assert config.project == "test-project"
    assert config.labels == {"environment": "test"}


def test_cli_passes_the_optional_parameters(run: MagicMock) -> None:
    """Checks that the CLI passes the optional parameters to the config."""
    main.run(
        [
            *BASE_ARGS,
            "--window-days",
            "7",
            "--single-ident-min-freq",
            "0.9",
            "--most-common-min-freq",
            "0.1",
            "--spoofing-threshold",
            "3",
        ]
    )

    config = run.call_args.args[0]
    assert config.window_days == 7
    assert config.single_ident_min_freq == 0.9
    assert config.most_common_min_freq == 0.1
    assert config.spoofing_threshold == 3


def test_cli_passes_the_dry_run(run: MagicMock) -> None:
    """Checks that the CLI passes the dry run option to the config."""
    main.run([*BASE_ARGS, "--dry-run"])

    assert run.call_args.args[0].dry_run is True


@pytest.mark.parametrize(
    "option",
    [
        "--bq-in-segment-identity-daily",
        "--bq-out-segment-vessel-daily",
        "--start-date",
        "--end-date",
        "--project",
        "--labels",
    ],
)
def test_cli_requires_the_required_options(run: MagicMock, option: str) -> None:
    """Checks that the CLI rejects a missing required option."""
    index = BASE_ARGS.index(option)
    args = [*BASE_ARGS[:index], *BASE_ARGS[index + 2:]]

    with pytest.raises(argparse.ArgumentTypeError, match=option.lstrip("-").replace("-", "_")):
        main.run(args)

    run.assert_not_called()


def test_cli_rejects_an_empty_date_range(run: MagicMock) -> None:
    """Checks that the end date is exclusive, so a range of one day is empty."""
    args = [a.replace("2024-01-08", "2024-01-01") for a in BASE_ARGS]

    with pytest.raises(PipelineConfigError, match="end_date"):
        main.run(args)

    run.assert_not_called()


@pytest.mark.parametrize(
    ("option", "value", "name"),
    [
        ("--single-ident-min-freq", "1.5", "single_ident_min_freq"),
        ("--most-common-min-freq", "-0.1", "most_common_min_freq"),
        ("--window-days", "-1", "window_days"),
        ("--spoofing-threshold", "-1", "spoofing_threshold"),
    ],
)
def test_cli_rejects_invalid_parameters(
    run: MagicMock, option: str, value: str, name: str
) -> None:
    """Checks that the CLI rejects frequencies out of range and negative amounts."""
    with pytest.raises(PipelineConfigError, match=name):
        main.run([*BASE_ARGS, option, value])

    run.assert_not_called()


def test_cli_reads_the_options_from_a_config_file(run: MagicMock, tmp_path: Path) -> None:
    """Checks that the options can come from a config file."""
    config_file = tmp_path / "config.yaml"
    config_file.write_text(
        "bq_in_segment_identity_daily: p.d.identity\n"
        "bq_out_segment_vessel_daily: p.d.out\n"
        "start_date: '2024-02-01'\n"
        "end_date: '2024-02-03'\n"
        "project: file-project\n"
        "window_days: 14\n"
        "labels:\n"
        "  environment: file\n"
    )

    main.run(["segment-vessel-daily", "-c", str(config_file)])

    config = run.call_args.args[0]
    assert config.bq_in_segment_identity_daily == "p.d.identity"
    assert config.start_date == date(2024, 2, 1)
    assert config.end_date == date(2024, 2, 3)
    assert config.window_days == 14
    assert config.project == "file-project"


def test_cli_rejects_unknown_options(run: MagicMock) -> None:
    """Checks that an option that the command does not define fails, as it has no Beam options."""
    with pytest.raises(SystemExit):
        main.run([*BASE_ARGS, "--unknown-option", "1"])

    run.assert_not_called()


def test_cli_rejects_unknown_config_file_keys(run: MagicMock, tmp_path: Path) -> None:
    """Checks that a config file key that the command does not define fails."""
    config_file = tmp_path / "config.yaml"
    config_file.write_text("unknown_key: 1\n")

    with pytest.raises(ValueError, match="not recognized"):
        main.run([*BASE_ARGS, "-c", str(config_file)])

    run.assert_not_called()
