"""Tests for the segment-info pipeline."""

from pytest_mock import MockerFixture

from pipe_segment.pipelines.segment_info import main
from pipe_segment.pipelines.segment_info.config import SegmentInfoConfig


def make_config() -> SegmentInfoConfig:
    """Returns a config with all the tables set."""
    return SegmentInfoConfig(
        project="test-project",
        bq_in_segment_identity_daily="project.dataset.segment_identity_daily",
        bq_in_segment_vessel_daily="project.dataset.segment_vessel_daily",
        bq_out_segment_info="project.dataset.segment_info",
        most_common_min_freq=0.1,
        labels={"environment": "test"},
    )


def test_run_queries_the_input_tables_into_the_output_table(mocker: MockerFixture) -> None:
    """Checks that the query reads the input tables and the job is labeled."""
    client_class = mocker.patch("pipe_segment.segment_info.segment_info.bigquery.Client")
    client = client_class.return_value

    main.run(make_config())

    client_class.assert_called_once_with(project="test-project")
    call = client.query_and_wait.call_args_list[-1]  # The last query fills the table.
    job_config = call.kwargs["job_config"]
    query = call.args[0]
    assert "`project.dataset.segment_identity_daily`" in query
    assert "`project.dataset.segment_vessel_daily`" in query
    assert job_config.labels == {"environment": "test"}
    assert job_config.destination.table_id == "segment_info"
