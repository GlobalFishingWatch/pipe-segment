"""Tests of what all the commands of the CLI have in common."""

import argparse

from pathlib import Path
from unittest.mock import MagicMock

import pytest

from pytest_mock import MockerFixture

from gfw.common.beam.pipeline.base import Pipeline
from gfw.common.config import PipelineConfigError
from pipe_segment.cli import main
from .base_args import (
    MESSAGES_SEGMENTED,
    SEGMENT_IDENTITY_DAILY,
    SEGMENT_INFO,
    SEGMENT_VESSEL,
    SEGMENT_VESSEL_DAILY,
    VESSEL_INFO,
)


BEAM_COMMANDS = [MESSAGES_SEGMENTED, SEGMENT_IDENTITY_DAILY]
QUERY_COMMANDS = [SEGMENT_VESSEL_DAILY, SEGMENT_VESSEL, SEGMENT_INFO, VESSEL_INFO]
DATE_COMMANDS = [MESSAGES_SEGMENTED, SEGMENT_IDENTITY_DAILY, SEGMENT_VESSEL_DAILY]


def name(args: list[str]) -> str:
    """Returns the name of the command of the arguments."""
    return args[0]


def required_options(args: list[str]) -> list[tuple[list[str], str]]:
    """Returns the arguments, and each of the options that the arguments have."""
    return [(args, option) for option in args[1::2]]


@pytest.fixture
def run(request: pytest.FixtureRequest, mocker: MockerFixture) -> MagicMock:
    """Mocks the run of the command that a test is about, so a test can never reach BigQuery."""
    command = name(request.node.callspec.params["args"]).replace("-", "_")
    return mocker.patch(f"pipe_segment.cli.commands.{command}.main.run")


@pytest.mark.parametrize(
    "args, option",
    [
        pytest.param(args, option, id=f"{name(args)}{option}")
        for args in [*BEAM_COMMANDS, *QUERY_COMMANDS]
        for args, option in required_options(args)
    ],
)
def test_cli_requires_the_required_options(
    run: MagicMock, args: list[str], option: str
) -> None:
    """Checks that the CLI rejects a command without one of its required options."""
    index = args.index(option)
    without_option = [*args[:index], *args[index + 2:]]

    with pytest.raises(argparse.ArgumentTypeError, match=option.lstrip("-").replace("-", "_")):
        main.run(without_option)

    run.assert_not_called()


@pytest.mark.parametrize("args", DATE_COMMANDS, ids=name)
def test_cli_rejects_an_empty_date_range(run: MagicMock, args: list[str]) -> None:
    """Checks that the end date is exclusive, so a range of one day is empty."""
    one_day = [a.replace("2024-01-08", "2024-01-01") for a in args]

    with pytest.raises(PipelineConfigError, match="end_date"):
        main.run(one_day)

    run.assert_not_called()


@pytest.mark.parametrize("args", QUERY_COMMANDS, ids=name)
def test_cli_rejects_unknown_options(run: MagicMock, args: list[str]) -> None:
    """Checks that an option that the command does not define fails, as it has no Beam options."""
    with pytest.raises(SystemExit):
        main.run([*args, "--unknown-option", "1"])

    run.assert_not_called()


@pytest.mark.parametrize("args", QUERY_COMMANDS, ids=name)
def test_cli_rejects_unknown_config_file_keys(
    run: MagicMock, args: list[str], tmp_path: Path
) -> None:
    """Checks that a config file key that the command does not define fails."""
    config_file = tmp_path / "config.yaml"
    config_file.write_text("unknown_key: 1\n")

    with pytest.raises(ValueError, match="not recognized"):
        main.run([*args, "-c", str(config_file)])

    run.assert_not_called()


@pytest.mark.parametrize("args", QUERY_COMMANDS, ids=name)
def test_a_query_command_runs_with_mocked_clients(args: list[str]) -> None:
    """Checks that the CLI reaches run, which builds and executes its queries on mocked clients.

    It is the integration of the CLI with the pipeline: nothing is patched, and the mocked
    clients keep it from reading or writing BigQuery.
    """
    main.run([*args, "--mock-bq-clients"])


@pytest.mark.parametrize("args", BEAM_COMMANDS, ids=name)
def test_a_beam_command_builds_its_pipeline_with_mocked_clients(
    mocker: MockerFixture, args: list[str]
) -> None:
    """Checks that the CLI reaches run, which builds the whole pipeline up to its launch.

    It is the integration of the CLI with the pipeline: only the launch of the job is patched,
    and the mocked clients keep it from reading or writing BigQuery.
    """
    pipeline_run = mocker.patch.object(Pipeline, "run", autospec=True)

    main.run([*args, "--mock-bq-clients"])

    pipeline_run.assert_called_once()
    pipeline = pipeline_run.call_args.args[0]
    assert isinstance(pipeline, Pipeline)
