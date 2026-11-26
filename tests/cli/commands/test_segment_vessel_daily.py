"""Tests for the segment-vessel-daily CLI command."""

from datetime import date
from unittest.mock import MagicMock

import pytest

from pytest_mock import MockerFixture

from pipe_segment.cli import main
from pipe_segment.pipelines.segment_vessel_daily.config import SegmentVesselDailyConfig
from .base_args import SEGMENT_VESSEL_DAILY as BASE_ARGS


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
