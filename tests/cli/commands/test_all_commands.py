"""Tests of what all the commands of the CLI have in common."""

import dataclasses

from unittest.mock import MagicMock

import pytest

from pytest_mock import MockerFixture

from gfw.common.beam.pipeline.base import Pipeline
from gfw.common.cli import labels_option
from pipe_segment.cli import main
from pipe_segment.cli.commands import (
    MessagesSegmented,
    SegmentIdentityDaily,
    SegmentInfo,
    SegmentVessel,
    SegmentVesselDaily,
    VesselInfo,
)
from pipe_segment.cli.options import mock_bq_clients_option, project_option
from pipe_segment.pipelines.messages_segmented.config import MessagesSegmentedConfig
from pipe_segment.pipelines.segment_identity_daily.config import SegmentIdentityDailyConfig
from pipe_segment.pipelines.segment_info.config import SegmentInfoConfig
from pipe_segment.pipelines.segment_vessel.config import SegmentVesselConfig
from pipe_segment.pipelines.segment_vessel_daily.config import SegmentVesselDailyConfig
from pipe_segment.pipelines.vessel_info.config import VesselInfoConfig
from .base_args import (
    MESSAGES_SEGMENTED,
    SEGMENT_IDENTITY_DAILY,
    SEGMENT_INFO,
    SEGMENT_VESSEL,
    SEGMENT_VESSEL_DAILY,
    VESSEL_INFO,
)


COMMANDS = [
    pytest.param(MessagesSegmented, MessagesSegmentedConfig, id="messages-segmented"),
    pytest.param(SegmentIdentityDaily, SegmentIdentityDailyConfig, id="segment-identity-daily"),
    pytest.param(SegmentVesselDaily, SegmentVesselDailyConfig, id="segment-vessel-daily"),
    pytest.param(SegmentVessel, SegmentVesselConfig, id="segment-vessel"),
    pytest.param(SegmentInfo, SegmentInfoConfig, id="segment-info"),
    pytest.param(VesselInfo, VesselInfoConfig, id="vessel-info"),
]
BEAM_COMMANDS = [MESSAGES_SEGMENTED, SEGMENT_IDENTITY_DAILY]
QUERY_COMMANDS = [SEGMENT_VESSEL_DAILY, SEGMENT_VESSEL, SEGMENT_INFO, VESSEL_INFO]


def name(args: list[str]) -> str:
    """Returns the name of the command of the arguments."""
    return args[0]


@pytest.fixture
def run(request: pytest.FixtureRequest, mocker: MockerFixture) -> MagicMock:
    """Mocks the run of the command that a test is about, so a test can never reach BigQuery."""
    command = name(request.node.callspec.params["args"]).replace("-", "_")
    return mocker.patch(f"pipe_segment.cli.commands.{command}.main.run")


@pytest.mark.parametrize("command, config_class", COMMANDS)
def test_the_options_of_a_command_agree_with_its_config(command, config_class) -> None:
    """Checks that the options and the fields of the config of a command match.

    Every option is a field, every field without a default has an option, and the default of an
    option is the one of its field: a different one would silently override the config's.
    """
    fields = {field.name: field for field in dataclasses.fields(config_class)}
    options = [*command().options, labels_option(), project_option(), mock_bq_clients_option()]
    dests = {option.dest for option in options}

    assert dests <= set(fields)
    required = {
        name
        for name, field in fields.items()
        if field.default is dataclasses.MISSING and field.default_factory is dataclasses.MISSING
    }
    assert required <= dests
    for option in options:
        field = fields[option.dest]
        if option.default is not None and field.default is not dataclasses.MISSING:
            assert option.default == field.default, option.dest


@pytest.mark.parametrize("args", QUERY_COMMANDS, ids=name)
def test_cli_rejects_unknown_options(run: MagicMock, args: list[str]) -> None:
    """Checks that an option that the command does not define fails, as it has no Beam options."""
    with pytest.raises(SystemExit):
        main.run([*args, "--unknown-option", "1"])

    run.assert_not_called()


@pytest.mark.parametrize("args", BEAM_COMMANDS, ids=name)
def test_a_beam_command_keeps_the_beam_options_that_it_does_not_define(
    run: MagicMock, args: list[str]
) -> None:
    """Checks that the Beam options reach the config as unparsed arguments."""
    main.run([*args, "--runner", "DataflowRunner", "--max_num_workers=7"])

    config = run.call_args.args[0]
    assert "--runner" in config.unknown_unparsed_args
    assert "--max_num_workers=7" in config.unknown_unparsed_args


@pytest.mark.parametrize("args", BEAM_COMMANDS, ids=name)
def test_a_beam_command_ignores_the_snake_case_wait_for_job_flag(
    run: MagicMock, args: list[str]
) -> None:
    """Checks that the flag that Airflow renders does not turn the wait off."""
    main.run([*args, "--wait_for_job"])

    config = run.call_args.args[0]
    assert config.wait_for_job is True
    assert "--wait_for_job" in config.unknown_unparsed_args


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
    assert isinstance(pipeline_run.call_args.args[0], Pipeline)
