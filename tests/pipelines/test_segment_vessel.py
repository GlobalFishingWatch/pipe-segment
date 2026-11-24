"""Tests for the segment-vessel pipeline."""

from typing import Any
from unittest.mock import MagicMock

import pytest

from google.cloud.bigquery import QueryJobConfig

from gfw.common.bigquery.helper import BigQueryHelper
from pipe_segment.pipelines.segment_vessel import main
from pipe_segment.pipelines.segment_vessel.config import SegmentVesselConfig


def make_config(**kwargs: Any) -> SegmentVesselConfig:
    """Returns a config with all the tables set."""
    defaults = {
        "project": "test-project",
        "bq_in_segment_vessel_daily": "project.dataset.segment_vessel_daily",
        "bq_out_segment_vessel": "project.dataset.segment_vessel",
        "labels": {"environment": "test"},
        "mock_bq_clients": True,
    }
    return SegmentVesselConfig(**(defaults | kwargs))


def run_with_client(config: SegmentVesselConfig) -> MagicMock:
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


def written_job_config(client: MagicMock) -> QueryJobConfig:
    """Returns the job config of the query that writes to the output table."""
    job_configs = [call.kwargs["job_config"] for call in client.query.call_args_list]
    return [job_config for job_config in job_configs if job_config.destination is not None][-1]


def test_run_queries_the_input_tables_into_the_output_table() -> None:
    """Checks that the query reads the input table and the job is labeled."""
    client = run_with_client(make_config())

    call = client.query.call_args_list[-1]
    job_config = written_job_config(client)
    assert "`project.dataset.segment_vessel_daily`" in call.args[0]
    assert job_config.destination.table_id == "segment_vessel"
    assert job_config.write_disposition == "WRITE_TRUNCATE"
    assert job_config.labels == {"environment": "test"}


@pytest.mark.parametrize("dry_run", [True, False])
def test_run_executes_the_query_in_dry_run_mode_if_asked(dry_run: bool) -> None:
    """Checks that the dry run option is passed to the BigQuery job."""
    client = run_with_client(make_config(dry_run=dry_run))

    assert client.default_query_job_config.dry_run is dry_run
