"""Tests for the queries of the messages-segmented pipeline."""

from datetime import date

from pipe_segment.pipelines.messages_segmented.queries import (
    FragmentsQuery,
    MessageDaysQuery,
    MessagesQuery,
    PriorSegmentsQuery,
)


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


def one_line(query: str) -> str:
    """Returns the query on one line."""
    return " ".join(query.split())


def test_fragments_are_read_with_their_times_in_seconds() -> None:
    """Checks that the times are cast to seconds, and the other columns are kept."""
    query = one_line(FragmentsQuery("project.dataset.fragments", date(2024, 1, 3)).render())

    assert "FROM `project.dataset.fragments`" in query
    assert "CAST(UNIX_MICROS(timestamp) AS FLOAT64) / 1000000 AS timestamp" in query
    assert "* EXCEPT ( timestamp, first_msg_timestamp, last_msg_timestamp )" in query


def test_all_the_fragments_are_read_without_a_start_date() -> None:
    """Checks that without a start date, the end date does not limit the query either."""
    query = FragmentsQuery("project.dataset.fragments", date(2024, 1, 3)).render()

    assert "WHERE" not in query


def test_the_fragments_of_the_days_are_read_with_a_start_date() -> None:
    """Checks that the days from the start to the end date are read, both included."""
    query = one_line(
        FragmentsQuery("project.dataset.fragments", date(2024, 1, 3), date(2024, 1, 1)).render()
    )

    assert "WHERE DATE(timestamp) >= '2024-01-01' AND DATE(timestamp) <= '2024-01-03'" in query


def test_prior_segments_are_read_from_the_day_in_both_tables() -> None:
    """Checks that the state of a day joins its segments with its fragments."""
    query = one_line(
        PriorSegmentsQuery(
            "project.dataset.segments", "project.dataset.fragments", date(2024, 4, 11)
        ).render()
    )

    assert "FROM `project.dataset.segments` AS s" in query
    assert "JOIN `project.dataset.fragments` AS f" in query
    assert "DATE(s.timestamp) = '2024-04-11'" in query
    assert "DATE(f.timestamp) = '2024-04-11'" in query
    assert "s.ssvid IN" not in query


def test_prior_segments_are_limited_to_the_ssvids_of_the_filter() -> None:
    """Checks that the ssvids are limited by the query of the filter."""
    query = one_line(
        PriorSegmentsQuery(
            "project.dataset.segments",
            "project.dataset.fragments",
            date(2024, 4, 11),
            "SELECT ssvid FROM t",
        ).render()
    )

    assert "AND s.ssvid IN (SELECT ssvid FROM t)" in query
