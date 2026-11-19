"""Tests for the vessel-info CLI command."""

import argparse

from pathlib import Path
from unittest.mock import MagicMock

import pytest

from pytest_mock import MockerFixture

from gfw.common.config import PipelineConfigError
from pipe_segment.cli import main
from pipe_segment.pipelines.vessel_info.config import VesselInfoConfig


BASE_ARGS = [
    "vessel-info",
    "--bq-in-segment-identity-daily",
    "project.dataset.segment_identity_daily",
    "--bq-in-segment-vessel-daily",
    "project.dataset.segment_vessel_daily",
    "--bq-out-vessel-info",
    "project.dataset.vessel_info",
    "--project",
    "test-project",
    "--labels",
    "environment=test",
]


@pytest.fixture
def run(mocker: MockerFixture) -> MagicMock:
    """Mocks the pipeline run that the CLI calls."""
    return mocker.patch("pipe_segment.cli.commands.vessel_info.main.run")


def test_cli_executes_run_with_the_config(run: MagicMock) -> None:
    """Checks that the CLI builds the config and passes it to run."""
    main.run(BASE_ARGS)

    config = run.call_args.args[0]
    assert isinstance(config, VesselInfoConfig)
    assert config.bq_in_segment_identity_daily == "project.dataset.segment_identity_daily"
    assert config.bq_in_segment_vessel_daily == "project.dataset.segment_vessel_daily"
    assert config.bq_out_vessel_info == "project.dataset.vessel_info"
    assert config.most_common_min_freq == 0.05
    assert config.project == "test-project"
    assert config.labels == {"environment": "test"}


def test_cli_passes_the_most_common_min_freq(run: MagicMock) -> None:
    """Checks that the CLI passes the minimal frequency to the config."""
    main.run([*BASE_ARGS, "--most-common-min-freq", "0.2"])

    assert run.call_args.args[0].most_common_min_freq == 0.2


@pytest.mark.parametrize(
    "option",
    [
        "--bq-in-segment-identity-daily",
        "--bq-in-segment-vessel-daily",
        "--bq-out-vessel-info",
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


@pytest.mark.parametrize("frequency", ["-0.1", "1.5"])
def test_cli_rejects_a_frequency_out_of_range(run: MagicMock, frequency: str) -> None:
    """Checks that the CLI rejects a most-common-min-freq outside 0 and 1."""
    with pytest.raises(PipelineConfigError, match="most_common_min_freq"):
        main.run([*BASE_ARGS, "--most-common-min-freq", frequency])

    run.assert_not_called()


def test_cli_reads_the_options_from_a_config_file(run: MagicMock, tmp_path: Path) -> None:
    """Checks that the options can come from a config file."""
    config_file = tmp_path / "config.yaml"
    config_file.write_text(
        "bq_in_segment_identity_daily: p.d.identity\n"
        "bq_in_segment_vessel_daily: p.d.vessel\n"
        "bq_out_vessel_info: p.d.out\n"
        "project: file-project\n"
        "most_common_min_freq: 0.3\n"
        "labels:\n"
        "  environment: file\n"
    )

    main.run(["vessel-info", "-c", str(config_file)])

    config = run.call_args.args[0]
    assert config.bq_in_segment_identity_daily == "p.d.identity"
    assert config.project == "file-project"
    assert config.most_common_min_freq == 0.3
    assert config.labels == {"environment": "file"}
