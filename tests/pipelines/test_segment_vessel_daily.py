"""Tests for the segment-vessel-daily pipeline."""

from datetime import date

from pytest_mock import MockerFixture

from pipe_segment.pipelines.segment_vessel_daily import main
from pipe_segment.pipelines.segment_vessel_daily.config import SegmentVesselDailyConfig


def make_config(start_date: date, end_date: date) -> SegmentVesselDailyConfig:
    """Returns a config with all the tables set."""
    return SegmentVesselDailyConfig(
        project="test-project",
        bq_in_segment_identity_daily="project.dataset.segment_identity_daily",
        bq_out_segment_vessel_daily="project.dataset.segment_vessel_daily",
        start_date=start_date,
        end_date=end_date,
        window_days=7,
        labels={"environment": "test"},
    )


def queries(client) -> list[str]:
    """Returns the queries that the client ran, in order."""
    return [call.args[0] for call in client.query_and_wait.call_args_list]


def test_run_processes_each_day_before_the_end_date(mocker: MockerFixture) -> None:
    """Checks that one query runs per day, and the end date is not processed."""
    client_class = mocker.patch("pipe_segment.segment_vessel.segment_vessel_daily.bigquery.Client")
    client = client_class.return_value

    main.run(make_config(date(2024, 1, 1), date(2024, 1, 3)))

    client_class.assert_called_once_with(project="test-project")
    clear, *days = queries(client)
    assert len(days) == 2
    assert "DATE('2024-01-01')" in days[0]
    assert "DATE('2024-01-02')" in days[1]
    assert "windowDays() AS (7)" in days[0]
    assert "`project.dataset.segment_identity_daily`" in days[0]
    labels = [call.kwargs["job_config"].labels for call in client.query_and_wait.call_args_list]
    assert labels == [{"environment": "test"}] * 3


def test_run_clears_the_days_before_the_end_date(mocker: MockerFixture) -> None:
    """Checks that the rows of the range are deleted, without the end date."""
    client_class = mocker.patch("pipe_segment.segment_vessel.segment_vessel_daily.bigquery.Client")
    client = client_class.return_value

    main.run(make_config(date(2024, 1, 1), date(2024, 1, 3)))

    clear = queries(client)[0]
    assert "DELETE FROM `project.dataset.segment_vessel_daily`" in clear
    assert ">= '2024-01-01'" in clear
    assert "<= '2024-01-02'" in clear
