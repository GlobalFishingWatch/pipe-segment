"""Tests for the segment-identity-daily pipeline."""

import json

from datetime import date

from apache_beam.options.pipeline_options import GoogleCloudOptions
from pytest_mock import MockerFixture

from gfw.common.beam.pipeline import Pipeline

from pipe_segment.pipelines.segment_identity_daily import main
from pipe_segment.pipelines.segment_identity_daily.config import SegmentIdentityDailyConfig
from pipe_segment.segment_identity.pipeline import SegmentIdentityPipeline
from pipe_segment.tools import timestamp_from_string


def make_config(**kwargs) -> SegmentIdentityDailyConfig:
    """Returns a config with all the tables set."""
    defaults = {
        "project": "test-project",
        "bq_in_segments": "project.dataset.segments",
        "bq_in_fragments": "project.dataset.fragments",
        "bq_out_segment_identity_daily": "project.dataset.segment_identity_daily",
        "start_date": date(2024, 1, 1),
        "end_date": date(2024, 1, 3),
        "labels": {"environment": "test"},
    }
    return SegmentIdentityDailyConfig(**{**defaults, **kwargs})


def test_run_resolves_the_beam_options_from_the_config(mocker: MockerFixture) -> None:
    """Checks that the project, the labels and the other Beam options reach the pipeline."""
    pipeline_class = mocker.patch.object(main, "SegmentIdentityPipeline")
    config = make_config(
        unknown_unparsed_args=("--max_num_workers=7",),
        unknown_parsed_args={"region": "us-central1"},
    )

    main.run(config)

    assert pipeline_class.call_args.args[0] == config
    beam_options = pipeline_class.call_args.args[1]
    cloud_options = beam_options.view_as(GoogleCloudOptions)
    assert cloud_options.project == "test-project"
    assert cloud_options.region == "us-central1"
    assert json.loads(cloud_options.labels[0]) == {"environment": "test"}
    assert beam_options.get_all_options()["max_num_workers"] == 7


def test_run_returns_the_result_of_the_pipeline(mocker: MockerFixture) -> None:
    """Checks that the exit code of the pipeline is returned."""
    pipeline_class = mocker.patch.object(main, "SegmentIdentityPipeline")
    pipeline_class.return_value.run.return_value = 1

    assert main.run(make_config()) == 1


def make_pipeline(mocker: MockerFixture) -> tuple[SegmentIdentityPipeline, object]:
    """Returns the pipeline, with a mocked BigQuery client, and the client."""
    client_class = mocker.patch("pipe_segment.segment_identity.pipeline.bigquery.Client")
    config = make_config()
    beam_options = Pipeline(project=config.project, labels=config.labels).pipeline_options
    return SegmentIdentityPipeline(config, beam_options), client_class.return_value


def test_the_jobs_are_labeled_with_the_labels_of_the_config(mocker: MockerFixture) -> None:
    """Checks that the labels do not depend on how Beam keeps them in its options."""
    pipeline, client = make_pipeline(mocker)

    pipeline.prepare_output_tables()

    assert pipeline.bq_helper.labels == {"environment": "test"}
    assert pipeline.source_segments().labels == {"environment": "test"}
    job_config = client.query_and_wait.call_args.kwargs["job_config"]
    assert job_config.labels == {"environment": "test"}


def test_the_end_date_is_not_read(mocker: MockerFixture) -> None:
    """Checks that the last day that the source reads is the one before the end date."""
    pipeline, _ = make_pipeline(mocker)

    source = pipeline.source_segments()

    assert source.start_ts == timestamp_from_string("2024-01-01")
    assert source.end_ts == timestamp_from_string("2024-01-02")


def test_the_end_date_is_not_cleared(mocker: MockerFixture) -> None:
    """Checks that the rows of the range are deleted, without the end date."""
    pipeline, client = make_pipeline(mocker)

    pipeline.prepare_output_tables()

    clear = client.query_and_wait.call_args.args[0]
    assert "DELETE FROM `project.dataset.segment_identity_daily`" in clear
    assert ">= '2024-01-01'" in clear
    assert "<= '2024-01-02'" in clear
