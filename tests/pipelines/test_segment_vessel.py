"""Tests for the segment-vessel pipeline."""

from pytest_mock import MockerFixture

from pipe_segment.pipelines.segment_vessel import main
from pipe_segment.pipelines.segment_vessel.config import SegmentVesselConfig


def make_config() -> SegmentVesselConfig:
    """Returns a config with all the tables set."""
    return SegmentVesselConfig(
        project="test-project",
        bq_in_segment_vessel_daily="project.dataset.segment_vessel_daily",
        bq_out_segment_vessel="project.dataset.segment_vessel",
        labels={"environment": "test"},
    )


def test_run_queries_the_input_table_into_the_output_table(mocker: MockerFixture) -> None:
    """Checks that the query reads the input table and the job is labeled."""
    client_class = mocker.patch("pipe_segment.segment_vessel.segment_vessel.bigquery.Client")
    client = client_class.return_value

    main.run(make_config())

    client_class.assert_called_once_with(project="test-project")
    call = client.query_and_wait.call_args_list[-1]  # The last query fills the table.
    job_config = call.kwargs["job_config"]
    assert "`project.dataset.segment_vessel_daily`" in call.args[0]
    assert job_config.labels == {"environment": "test"}
    assert job_config.destination.table_id == "segment_vessel"
