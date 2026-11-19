"""Tests for the messages-segmented CLI command."""

import argparse

from datetime import date
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from pytest_mock import MockerFixture

from gfw.common.config import PipelineConfigError
from pipe_segment.cli import main
from pipe_segment.pipelines.messages_segmented.config import MessagesSegmentedConfig


BASE_ARGS = [
    "messages-segmented",
    "--bq-in-normalized-messages",
    "project.dataset.messages",
    "--bq-out-segmented-messages",
    "project.dataset.messages_segmented",
    "--bq-out-segments",
    "project.dataset.segments",
    "--bq-out-fragments",
    "project.dataset.fragments",
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
    return mocker.patch("pipe_segment.cli.commands.messages_segmented.main.run")


def test_cli_executes_run_with_the_config(run: MagicMock) -> None:
    """Checks that the CLI builds the config and passes it to run."""
    main.run(BASE_ARGS)

    config = run.call_args.args[0]
    assert isinstance(config, MessagesSegmentedConfig)
    assert config.bq_in_normalized_messages == "project.dataset.messages"
    assert config.bq_out_segmented_messages == "project.dataset.messages_segmented"
    assert config.bq_out_segments == "project.dataset.segments"
    assert config.bq_out_fragments == "project.dataset.fragments"
    assert config.start_date == date(2024, 1, 1)
    assert config.end_date == date(2024, 1, 8)
    assert config.incremental is False
    assert config.wait_for_job is True
    assert config.bq_in_segments is None
    assert config.bq_in_fragments is None
    assert config.sat_offsets_enabled is False
    assert config.project == "test-project"
    assert config.labels == {"environment": "test"}


def test_cli_passes_the_incremental_options(run: MagicMock) -> None:
    """Checks that the incremental flag and the input tables reach the config."""
    main.run(
        [
            *BASE_ARGS,
            "--incremental",
            "--bq-in-segments",
            "project.dataset.in_segments",
            "--bq-in-fragments",
            "project.dataset.in_fragments",
        ]
    )

    config = run.call_args.args[0]
    assert config.incremental is True
    assert config.bq_in_segments == "project.dataset.in_segments"
    assert config.bq_in_fragments == "project.dataset.in_fragments"


def test_cli_passes_the_satellite_correction_options(run: MagicMock) -> None:
    """Checks that the satellite inputs and output reach the config."""
    main.run(
        [
            *BASE_ARGS,
            "--bq-in-sat-offset-messages",
            "project.dataset.sat_messages",
            "--bq-in-norad-to-receiver",
            "project.dataset.norad",
            "--bq-in-sat-positions",
            "project.dataset.positions",
            "--bq-out-sat-offsets",
            "project.dataset.sat_offsets",
            "--bad-hour-padding",
            "2",
            "--max-timing-offset-s",
            "10",
        ]
    )

    config = run.call_args.args[0]
    assert config.sat_offsets_enabled is True
    assert config.bq_out_sat_offsets == "project.dataset.sat_offsets"
    assert config.bad_hour_padding == 2
    assert config.max_timing_offset_s == 10


def test_cli_passes_the_other_options(run: MagicMock) -> None:
    """Checks the remaining options and the Beam options that the CLI does not define."""
    main.run(
        [
            *BASE_ARGS,
            "--bins-per-day",
            "8",
            "--ssvid-filter-query",
            "SELECT ssvid FROM t",
            "--no-wait-for-job",
            "--runner",
            "DataflowRunner",
        ]
    )

    config = run.call_args.args[0]
    assert config.bins_per_day == 8
    assert config.ssvid_filter_query == "SELECT ssvid FROM t"
    assert config.wait_for_job is False
    assert "--runner" in config.unknown_unparsed_args


@pytest.mark.parametrize(
    "option",
    [
        "--bq-in-normalized-messages",
        "--bq-out-segmented-messages",
        "--bq-out-segments",
        "--bq-out-fragments",
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
        "bq_in_normalized_messages: p.d.messages\n"
        "bq_out_segmented_messages: p.d.messages_segmented\n"
        "bq_out_segments: p.d.segments\n"
        "bq_out_fragments: p.d.fragments\n"
        "start_date: '2024-02-01'\n"
        "end_date: '2024-02-03'\n"
        "incremental: true\n"
        "project: file-project\n"
        "max_num_workers: 50\n"
        "labels:\n"
        "  environment: file\n"
    )

    main.run(["messages-segmented", "-c", str(config_file)])

    config = run.call_args.args[0]
    assert config.bq_in_normalized_messages == "p.d.messages"
    assert config.incremental is True
    assert config.project == "file-project"
    assert config.unknown_parsed_args["max_num_workers"] == 50
