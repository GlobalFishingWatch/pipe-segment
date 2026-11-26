"""Tests for the table of the segment-identity-daily pipeline."""

from datetime import date

from pipe_segment.pipelines.segment_identity_daily.table_config import (
    SegmentIdentityDailyTableConfig,
)


def make_table_config() -> SegmentIdentityDailyTableConfig:
    """Returns the config of a table."""
    return SegmentIdentityDailyTableConfig(table_id="project.dataset.segment_identity_daily")


def test_the_table_has_the_layout_of_the_legacy_table() -> None:
    """Checks that the table is partitioned by month and clustered by its day."""
    table_config = make_table_config()

    assert table_config.partition_type == "MONTH"
    assert table_config.partition_field == "summary_timestamp"
    assert table_config.clustering_fields == ("summary_timestamp",)


def test_the_schema_has_the_fields_of_the_summaries() -> None:
    """Checks that the schema is read, with the summary_timestamp that partitions the table."""
    names = [field["name"] for field in make_table_config().schema]

    assert len(names) == 18
    assert "summary_timestamp" in names
    assert "timestamp" not in names


def test_delete_query_deletes_the_days_before_the_end_date() -> None:
    """Checks that the range is deleted without its end date."""
    query = make_table_config().delete_query(date(2024, 1, 1), date(2024, 1, 3))

    assert query == (
        "DELETE FROM `project.dataset.segment_identity_daily` "
        "WHERE DATE(summary_timestamp) >= '2024-01-01' AND DATE(summary_timestamp) < '2024-01-03'"
    )
