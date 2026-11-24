"""Tests for the tables of the messages-segmented pipeline."""

from datetime import date

import pytest

from pipe_segment.pipelines.messages_segmented.table_config import (
    DatePartitionedTableConfig,
    FragmentsTableConfig,
    SatelliteOffsetsTableConfig,
    SegmentedMessagesTableConfig,
    SegmentsTableConfig,
)


@pytest.mark.parametrize(
    ("table_config_class", "field"),
    [
        (SegmentedMessagesTableConfig, "timestamp"),
        (FragmentsTableConfig, "timestamp"),
        (SegmentsTableConfig, "timestamp"),
        (SatelliteOffsetsTableConfig, "hour"),
    ],
)
def test_the_tables_have_the_layout_of_the_legacy_tables(
    table_config_class: type[DatePartitionedTableConfig], field: str
) -> None:
    """Checks that each table is partitioned by month and clustered by its time field."""
    table_config = table_config_class(table_id="project.dataset.table")

    assert table_config.partition_type == "MONTH"
    assert table_config.partition_field == field
    assert table_config.clustering_fields == (field,)
    assert field in [column["name"] for column in table_config.schema]


def test_delete_query_deletes_the_days_before_the_end_date() -> None:
    """Checks that the range is deleted without its end date."""
    table_config = SegmentsTableConfig(table_id="project.dataset.segments")

    assert table_config.delete_query(date(2024, 1, 1), date(2024, 1, 3)) == (
        "DELETE FROM `project.dataset.segments` "
        "WHERE DATE(timestamp) >= '2024-01-01' AND DATE(timestamp) < '2024-01-03'"
    )


def test_delete_query_of_the_satellite_offsets_uses_the_hour() -> None:
    """Checks that the offsets are deleted by the day of their hour."""
    table_config = SatelliteOffsetsTableConfig(table_id="project.dataset.offsets")

    assert "DATE(hour) >= '2024-01-01' AND DATE(hour) < '2024-01-03'" in table_config.delete_query(
        date(2024, 1, 1), date(2024, 1, 3)
    )
