"""Tests for the graph of the messages-segmented pipeline."""

import datetime as dt

from datetime import date
from typing import Any
from unittest.mock import MagicMock

import apache_beam as beam
import pytest

from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.pipeline import PipelineVisitor

from pipe_segment.pipelines.messages_segmented import main
from pipe_segment.pipelines.messages_segmented.config import MessagesSegmentedConfig
from pipe_segment.pipelines.messages_segmented.dag import (
    MessagesSegmentedDag,
    safe_date,
    time_bin_key,
    time_bin_ndx,
)


SAT_OFFSETS = {
    "bq_in_sat_offset_messages": "project.dataset.sat_messages",
    "bq_in_norad_to_receiver": "project.dataset.norad",
    "bq_in_sat_positions": "project.dataset.sat_positions",
    "bq_out_sat_offsets": "my-project:dataset.sat_offsets",
}


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


class Labels(PipelineVisitor):
    """Collects the labels of the transforms of a pipeline."""

    def __init__(self) -> None:
        """Starts with no labels."""
        self.labels: set[str] = set()

    def enter_composite_transform(self, transform_node: Any) -> None:
        """Collects the label of a composite transform."""
        self.labels.add(transform_node.full_label)

    def visit_transform(self, transform_node: Any) -> None:
        """Collects the label of a transform."""
        self.labels.add(transform_node.full_label)


def applied_labels(config: MessagesSegmentedConfig) -> set[str]:
    """Builds the graph of the config, without running it, and returns its labels."""
    bq_helper = MagicMock(labels=config.labels)
    bq_helper.fetch_table.return_value = MagicMock(
        time_partitioning=MagicMock(type_="DAY", field="timestamp")
    )
    messages_table, segments_table, fragments_table, *_ = main.table_configs(config)
    dag = MessagesSegmentedDag(
        config,
        bq_helper,
        messages_sink=main.sink(messages_table, lambda **kwargs: beam.Map(lambda x: x)),
        segments_sink=main.sink(segments_table, lambda **kwargs: beam.Map(lambda x: x)),
        fragments_sink=main.sink(fragments_table, lambda **kwargs: beam.Map(lambda x: x)),
    )
    pipeline = beam.Pipeline(options=PipelineOptions(["--project", "test-project"]))

    dag.apply(pipeline)

    visitor = Labels()
    pipeline.visit(visitor)
    return visitor.labels


@pytest.mark.parametrize("incremental", [False, True])
def test_the_graph_writes_the_messages_the_fragments_and_the_segments(incremental: bool) -> None:
    """Checks that the graph has the steps of the segmentation, in both modes."""
    labels = applied_labels(make_config(incremental=incremental))

    assert {"Fragment", "Segmenter", "WriteFragments", "WriteMessages", "WriteSegments"} <= labels
    assert "WriteSatOffsets" not in " ".join(labels)


def test_the_graph_corrects_the_satellite_times_if_the_offsets_are_given() -> None:
    """Checks that the offsets are computed, written and used to filter the messages."""
    labels = applied_labels(make_config(**SAT_OFFSETS))

    joined = " ".join(labels)
    assert "SatOffsets" in joined
    assert "WriteSatOffsets" in joined
    assert "FilterBadSatelliteTimes" in joined


def test_safe_date_is_the_utc_date_of_the_timestamp() -> None:
    """Checks that a timestamp becomes its UTC date, and a missing one stays missing."""
    assert safe_date(1704067200.0) == date(2024, 1, 1)
    assert safe_date(None) is None


@pytest.mark.parametrize(("hour", "expected"), [(0, 0), (5, 0), (6, 1), (17, 2), (23, 3)])
def test_time_bin_ndx_splits_the_day_in_bins(hour: int, expected: int) -> None:
    """Checks that the hours of the day fall in four bins of six hours."""
    dtime = dt.datetime(2024, 1, 1, hour, 30, tzinfo=dt.timezone.utc)

    assert time_bin_ndx(dtime, 4) == expected


def test_time_bin_key_has_the_ssvid_the_fragment_the_day_and_the_bin() -> None:
    """Checks the key that tags a message."""
    message = {"ssvid": "123", "frag_id": "123-1", "timestamp": 1704067200.0 + 7 * 3600}

    assert time_bin_key(message, 4) == ("123", "123-1", "2024-01-01", 1)
