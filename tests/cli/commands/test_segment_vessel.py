"""Tests for the segment-vessel CLI command."""

from unittest.mock import MagicMock

import pytest

from pytest_mock import MockerFixture

from pipe_segment.cli import main
from pipe_segment.pipelines.segment_vessel.config import SegmentVesselConfig
from .base_args import SEGMENT_VESSEL as BASE_ARGS


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
