"""Tests for the segment-vessel pipeline."""

from typing import Any
from unittest.mock import MagicMock

from .. import query_helpers
from ..query_helpers import written_job_config
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
    return query_helpers.run_with_client(main, config)


def test_run_queries_the_input_tables_into_the_output_table() -> None:
    """Checks that the query reads the input table and the job is labeled."""
    client = run_with_client(make_config())

    call = client.query.call_args_list[-1]
    job_config = written_job_config(client)
    assert "`project.dataset.segment_vessel_daily`" in call.args[0]
    assert job_config.destination.table_id == "segment_vessel"
    assert job_config.write_disposition == "WRITE_TRUNCATE"
    assert job_config.labels == {"environment": "test"}
