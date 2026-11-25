"""Tests for the messages-segmented pipeline config and entry point."""

import json

from datetime import date

import pytest

from apache_beam.options.pipeline_options import GoogleCloudOptions
from pytest_mock import MockerFixture

from gfw.common.config import PipelineConfigError
from pipe_segment.pipelines.messages_segmented import main
from pipe_segment.pipelines.messages_segmented.config import MessagesSegmentedConfig


def make_config(**kwargs) -> MessagesSegmentedConfig:
    """Returns a config with the required fields."""
    defaults = {
        "project": "test-project",
        "bq_in_normalized_messages": "project.dataset.messages",
        "bq_out_segmented_messages": "project.dataset.messages_segmented",
        "bq_out_segments": "project.dataset.segments",
        "bq_out_fragments": "project.dataset.fragments",
        "start_date": date(2024, 1, 1),
        "end_date": date(2024, 1, 3),
        "labels": {"environment": "test"},
    }
    return MessagesSegmentedConfig(**{**defaults, **kwargs})


def test_the_defaults() -> None:
    """Checks the defaults of the optional fields."""
    config = make_config()

    assert config.bad_hour_padding == 1
    assert config.max_timing_offset_s == 30
    assert config.bins_per_day == 4
    assert config.incremental is False
    assert config.wait_for_job is True
    assert config.bq_in_segments is None
    assert config.bq_in_fragments is None
    assert config.ssvid_filter_query is None


@pytest.mark.parametrize(
    ("kwargs", "name"),
    [
        ({"bad_hour_padding": -1}, "bad_hour_padding"),
        ({"max_timing_offset_s": -1}, "max_timing_offset_s"),
        ({"bins_per_day": 0}, "bins_per_day"),
        ({"end_date": date(2024, 1, 1)}, "end_date"),
    ],
)
def test_the_config_rejects_invalid_values(kwargs: dict, name: str) -> None:
    """Checks that invalid amounts and an empty date range raise."""
    with pytest.raises(PipelineConfigError, match=name):
        make_config(**kwargs)


def test_the_satellite_correction_needs_all_its_inputs() -> None:
    """Checks that the satellite offsets are computed only with all the inputs."""
    inputs = {
        "bq_in_sat_offset_messages": "p.d.messages",
        "bq_in_norad_to_receiver": "p.d.norad",
        "bq_in_sat_positions": "p.d.positions",
    }

    assert make_config(**inputs).sat_offsets_enabled is True
    assert make_config().sat_offsets_enabled is False
    for name in inputs:
        partial = {key: value for key, value in inputs.items() if key != name}
        assert make_config(**partial).sat_offsets_enabled is False


def test_run_resolves_the_beam_options_from_the_config(mocker: MockerFixture) -> None:
    """Checks that the project, the labels and the other Beam options reach the pipeline."""
    pipeline_class = mocker.patch.object(main, "SegmentPipeline")
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
    pipeline_class = mocker.patch.object(main, "SegmentPipeline")
    pipeline_class.return_value.run.return_value = 1

    assert main.run(make_config()) == 1
