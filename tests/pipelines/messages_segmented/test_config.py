"""Tests for the messages-segmented pipeline config."""

from datetime import date
from typing import Any

import pytest

from gfw.common.config import PipelineConfigError
from pipe_segment.pipelines.messages_segmented.config import MessagesSegmentedConfig


def make_config(**kwargs: Any) -> MessagesSegmentedConfig:
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


def test_the_input_tables_default_to_the_output_tables() -> None:
    """Checks that the state is read from the tables that the run writes."""
    config = make_config()

    assert config.in_segments_table == "project.dataset.segments"
    assert config.in_fragments_table == "project.dataset.fragments"


def test_the_input_tables_can_be_read_from_other_tables() -> None:
    """Checks that the state can be read from other tables."""
    config = make_config(
        bq_in_segments="project.dataset.in_segments",
        bq_in_fragments="project.dataset.in_fragments",
    )

    assert config.in_segments_table == "project.dataset.in_segments"
    assert config.in_fragments_table == "project.dataset.in_fragments"
