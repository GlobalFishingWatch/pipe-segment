"""Tests for the source tables of the messages."""

from datetime import date

import pytest

from google.api_core.exceptions import NotFound
from google.cloud import bigquery

from gfw.common.bigquery.helper import BigQueryHelper
from pipe_segment.pipelines.messages_segmented.bq_source import BigQuerySource
from pipe_segment.pipelines.messages_segmented.transforms.read_messages import (
    build_days_query,
    build_filter,
)


def make_bq_helper(partition_field: str | None = "timestamp") -> BigQueryHelper:
    """Returns a helper whose tables are partitioned by a field, or not partitioned."""
    bq_helper = BigQueryHelper.mocked(project="test-project")

    def get_table(table_id: str) -> bigquery.Table:
        table = bigquery.Table(table_id)
        if partition_field is not None:
            table.time_partitioning = bigquery.table.TimePartitioning(field=partition_field)
        return table

    bq_helper.client.get_table.side_effect = get_table
    return bq_helper


def test_the_source_is_filtered_by_its_partition_field() -> None:
    """Checks that the days of the source are the ones of its partition field."""
    source = BigQuerySource(make_bq_helper("received_at"), "project.dataset.messages")

    assert source.filtering_field == "DATE(received_at)"


def test_the_source_is_filtered_by_timestamp_if_it_has_no_partition_field() -> None:
    """Checks that a table that is not partitioned by a field uses its timestamp."""
    source = BigQuerySource(make_bq_helper(None), "project.dataset.messages")

    assert source.filtering_field == "DATE(timestamp)"


def test_a_source_that_does_not_exist_fails() -> None:
    """Checks that the lookup does not hide a missing table."""
    bq_helper = BigQueryHelper.mocked(project="test-project")
    bq_helper.client.get_table.side_effect = NotFound("Not found")

    with pytest.raises(NotFound):
        BigQuerySource(bq_helper, "project.dataset.messages")


def test_the_filter_keeps_the_days_between_the_dates() -> None:
    """Checks that both dates are included."""
    source = BigQuerySource(make_bq_helper(), "project.dataset.messages")

    condition = build_filter(source, date(2024, 1, 1), date(2024, 1, 3))

    assert condition == "DATE(timestamp) BETWEEN '2024-01-01' AND '2024-01-03'"


def test_the_days_query_counts_the_days_of_all_the_sources() -> None:
    """Checks that the days are counted over all the sources together."""
    sources = [
        BigQuerySource(make_bq_helper(), "project.dataset.one"),
        BigQuerySource(make_bq_helper(), "project.dataset.two"),
    ]

    query = " ".join(build_days_query(sources, date(2024, 1, 1), date(2024, 1, 3)).split())

    assert query.startswith("SELECT COUNT(DISTINCT day) FROM (")
    assert "FROM `project.dataset.one` WHERE DATE(timestamp) BETWEEN" in query
    assert "FROM `project.dataset.two` WHERE DATE(timestamp) BETWEEN" in query
    assert "UNION ALL" in query
