"""Tests for the segment-vessel-daily pipeline."""

from datetime import date
from typing import Any
from unittest.mock import MagicMock

from google.cloud.bigquery import QueryJobConfig

from gfw.common.bigquery.helper import BigQueryHelper
from pipe_segment.pipelines.segment_vessel_daily import main
from pipe_segment.pipelines.segment_vessel_daily.config import SegmentVesselDailyConfig


def make_config(**kwargs: Any) -> SegmentVesselDailyConfig:
    """Returns a config with all the tables set."""
    defaults = {
        "project": "test-project",
        "bq_in_segment_identity_daily": "project.dataset.segment_identity_daily",
        "bq_out_segment_vessel_daily": "project.dataset.segment_vessel_daily",
        "start_date": date(2024, 1, 1),
        "end_date": date(2024, 1, 3),
        "window_days": 7,
        "labels": {"environment": "test"},
        "mock_bq_clients": True,
    }
    return SegmentVesselDailyConfig(**(defaults | kwargs))


def run_with_client(config: SegmentVesselDailyConfig) -> MagicMock:
    """Runs the pipeline and returns the mocked BigQuery client it used."""
    mocked_factory = BigQueryHelper.get_client_factory(mocked=True)
    clients = []

    def client_factory(**kwargs: Any) -> MagicMock:
        client = mocked_factory(**kwargs)
        client.default_query_job_config = kwargs["default_query_job_config"]
        clients.append(client)
        return client

    main.run(config, bq_client_factory=client_factory)
    return clients[0]


def job_configs(client: MagicMock) -> list[QueryJobConfig]:
    """Returns the job configs of the queries that the client ran, in order."""
    return [call.kwargs["job_config"] for call in client.query.call_args_list]


def queries(client: MagicMock) -> list[str]:
    """Returns the queries that the client ran, in order."""
    return [call.args[0] for call in client.query.call_args_list]


def test_run_processes_each_day_before_the_end_date() -> None:
    """Checks that one query runs per day, and the end date is not processed."""
    client = run_with_client(make_config())

    _, *days = queries(client)
    assert len(days) == 2
    assert "DATE('2024-01-01')" in days[0]
    assert "DATE('2024-01-02')" in days[1]
    assert "windowDays() AS (7)" in days[0]
    assert "`project.dataset.segment_identity_daily`" in days[0]


def test_run_appends_each_day_to_the_output_table() -> None:
    """Checks that the days are appended to the output table, with its layout, and labeled."""
    client = run_with_client(make_config())

    _, *days = job_configs(client)
    for job_config in days:
        assert job_config.destination.table_id == "segment_vessel_daily"
        assert job_config.write_disposition == "WRITE_APPEND"
        assert job_config.time_partitioning.type_ == "MONTH"
        assert job_config.time_partitioning.field == "day"
        assert job_config.clustering_fields == ["day"]
        assert job_config.labels == {"environment": "test"}


def test_run_creates_the_output_table_with_its_layout() -> None:
    """Checks that the table is created if needed, partitioned and clustered."""
    client = run_with_client(make_config())

    table = client.create_table.call_args.args[0]
    assert client.create_table.call_args.kwargs == {"exists_ok": True}
    assert table.time_partitioning.field == "day"
    assert table.clustering_fields == ["day"]
    assert table.labels == {"environment": "test"}


def test_run_deletes_the_days_before_the_end_date() -> None:
    """Checks that the rows of the range are deleted first, without the end date."""
    client = run_with_client(make_config())

    delete = queries(client)[0]
    assert "DELETE FROM `project.dataset.segment_vessel_daily`" in delete
    assert ">= '2024-01-01'" in delete
    assert "< '2024-01-03'" in delete
    assert job_configs(client)[0].labels == {"environment": "test"}


def test_run_in_dry_run_only_runs_the_queries_of_the_days() -> None:
    """Checks that a dry run neither creates the table nor deletes rows."""
    client = run_with_client(make_config(dry_run=True))

    assert client.default_query_job_config.dry_run is True
    client.create_table.assert_not_called()
    assert len(queries(client)) == 2
    assert all("DELETE FROM" not in query for query in queries(client))
