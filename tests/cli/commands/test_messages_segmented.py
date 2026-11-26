"""Tests for the messages-segmented CLI command."""

from datetime import date
from unittest.mock import MagicMock

import pytest

from pytest_mock import MockerFixture

from pipe_segment.cli import main
from pipe_segment.pipelines.messages_segmented.config import MessagesSegmentedConfig
from .base_args import MESSAGES_SEGMENTED as BASE_ARGS


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
