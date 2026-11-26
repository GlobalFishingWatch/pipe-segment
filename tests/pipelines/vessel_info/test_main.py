"""Tests for the vessel-info pipeline."""

from typing import Any
from unittest.mock import MagicMock

from .. import query_helpers
from ..query_helpers import written_job_config
from pipe_segment.pipelines.vessel_info import main
from pipe_segment.pipelines.vessel_info.config import VesselInfoConfig


def make_config(**kwargs: Any) -> VesselInfoConfig:
    """Returns a config with all the tables set."""
    defaults = {
        "project": "test-project",
        "bq_in_segment_identity_daily": "project.dataset.segment_identity_daily",
        "bq_in_segment_vessel_daily": "project.dataset.segment_vessel_daily",
        "bq_out_vessel_info": "project.dataset.vessel_info",
        "most_common_min_freq": 0.1,
        "labels": {"environment": "test"},
        "mock_bq_clients": True,
    }
    return VesselInfoConfig(**(defaults | kwargs))


def run_with_client(config: VesselInfoConfig) -> MagicMock:
    """Runs the pipeline and returns the mocked BigQuery client it used."""
    return query_helpers.run_with_client(main, config)


def test_run_queries_the_input_tables_into_the_output_table() -> None:
    """Checks that the query reads the input tables and the job is labeled."""
    client = run_with_client(make_config())

    call = client.query.call_args_list[-1]
    job_config = written_job_config(client)
    assert "`project.dataset.segment_identity_daily`" in call.args[0]
    assert "`project.dataset.segment_vessel_daily`" in call.args[0]
    assert job_config.destination.table_id == "vessel_info"
    assert job_config.write_disposition == "WRITE_TRUNCATE"
    assert job_config.labels == {"environment": "test"}


def test_run_passes_the_most_common_min_freq_to_the_query() -> None:
    """Checks that the minimal frequency is rendered in the query."""
    client = run_with_client(make_config(most_common_min_freq=0.25))

    assert "mostCommonMinFreq() AS (0.25)" in client.query.call_args_list[-1].args[0]
