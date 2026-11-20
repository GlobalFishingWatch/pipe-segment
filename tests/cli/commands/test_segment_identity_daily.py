"""Tests for the segment-identity-daily CLI command."""

import argparse

from datetime import date
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from pytest_mock import MockerFixture

from gfw.common.config import PipelineConfigError
from pipe_segment.cli import main
from pipe_segment.pipelines.segment_identity_daily.config import SegmentIdentityDailyConfig


BASE_ARGS = [
    "segment-identity-daily",
    "--bq-in-segments",
    "project.dataset.segments",
    "--bq-in-fragments",
    "project.dataset.fragments",
    "--bq-out-segment-identity-daily",
    "project.dataset.segment_identity_daily",
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
    return mocker.patch("pipe_segment.cli.commands.segment_identity_daily.main.run")


def test_cli_executes_run_with_the_config(run: MagicMock) -> None:
    """Checks that the CLI builds the config and passes it to run."""
    main.run(BASE_ARGS)

    config = run.call_args.args[0]
    assert isinstance(config, SegmentIdentityDailyConfig)
    assert config.bq_in_segments == "project.dataset.segments"
    assert config.bq_in_fragments == "project.dataset.fragments"
    assert config.bq_out_segment_identity_daily == "project.dataset.segment_identity_daily"
    assert config.start_date == date(2024, 1, 1)
    assert config.end_date == date(2024, 1, 8)
    assert config.wait_for_job is False
    assert config.project == "test-project"
    assert config.labels == {"environment": "test"}


def test_cli_passes_the_wait_for_job_flag(run: MagicMock) -> None:
    """Checks that the CLI passes the wait flag to the config."""
    main.run([*BASE_ARGS, "--wait-for-job"])

    assert run.call_args.args[0].wait_for_job is True


def test_cli_keeps_the_beam_options_that_it_does_not_define(run: MagicMock) -> None:
    """Checks that the Beam options reach the config as unparsed arguments."""
    main.run([*BASE_ARGS, "--runner", "DataflowRunner", "--max_num_workers=7"])

    config = run.call_args.args[0]
    assert "--runner" in config.unknown_unparsed_args
    assert "--max_num_workers=7" in config.unknown_unparsed_args


@pytest.mark.parametrize(
    "option",
    [
        "--bq-in-segments",
        "--bq-in-fragments",
        "--bq-out-segment-identity-daily",
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


def test_cli_reads_the_options_from_a_config_file(run: MagicMock, tmp_path: Path) -> None:
    """Checks that the options, including the Beam ones, can come from a config file."""
    config_file = tmp_path / "config.yaml"
    config_file.write_text(
        "bq_in_segments: p.d.segments\n"
        "bq_in_fragments: p.d.fragments\n"
        "bq_out_segment_identity_daily: p.d.out\n"
        "start_date: '2024-02-01'\n"
        "end_date: '2024-02-03'\n"
        "project: file-project\n"
        "max_num_workers: 50\n"
        "labels:\n"
        "  environment: file\n"
    )

    main.run(["segment-identity-daily", "-c", str(config_file)])

    config = run.call_args.args[0]
    assert config.bq_in_segments == "p.d.segments"
    assert config.start_date == date(2024, 2, 1)
    assert config.project == "file-project"
    assert config.unknown_parsed_args["max_num_workers"] == 50
