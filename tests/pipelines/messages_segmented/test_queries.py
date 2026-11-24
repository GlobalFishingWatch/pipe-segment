"""Tests for the queries of the messages-segmented pipeline."""

from datetime import date

from pipe_segment.pipelines.messages_segmented.queries import MessageDaysQuery, MessagesQuery


def messages_query(**kwargs: object) -> str:
    """Returns the rendered messages query, on one line."""
    fields = {
        "source": "project.dataset.messages",
        "start_date": date(2024, 1, 1),
        "end_date": date(2024, 1, 3),
    }
    return " ".join(MessagesQuery(**(fields | kwargs)).render().split())


def test_messages_are_read_from_the_days_of_the_range() -> None:
    """Checks that the days are filtered by the timestamp, which is also cast to seconds."""
    query = messages_query()

    assert "FROM `project.dataset.messages`" in query
    assert "DATE(timestamp) BETWEEN '2024-01-01' AND '2024-01-03'" in query
    assert "CAST(UNIX_MICROS(timestamp) AS FLOAT64) / 1000000 AS timestamp" in query
    assert "ssvid IN" not in query


def test_messages_can_be_filtered_by_another_partition_field() -> None:
    """Checks that the field of the days can be changed."""
    query = messages_query(partition_field="received_at")

    assert "DATE(received_at) BETWEEN '2024-01-01' AND '2024-01-03'" in query


def test_messages_are_limited_to_the_ssvids_of_the_filter() -> None:
    """Checks that the ssvids are limited by the query of the filter."""
    query = messages_query(ssvid_filter_query="SELECT ssvid FROM t")

    assert "WHERE ssvid IN (SELECT ssvid FROM t)" in query


def test_the_days_with_messages_are_counted() -> None:
    """Checks that the distinct days of the source between the dates are counted."""
    query = " ".join(
        MessageDaysQuery("project.dataset.messages", date(2024, 1, 1), date(2024, 1, 3))
        .render()
        .split()
    )

    assert query == (
        "SELECT COUNT(DISTINCT DATE(timestamp)) FROM `project.dataset.messages` "
        "WHERE DATE(timestamp) BETWEEN '2024-01-01' AND '2024-01-03'"
    )
