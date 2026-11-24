"""Tests for the queries of the segment-identity-daily pipeline."""

from datetime import date

from pipe_segment.pipelines.segment_identity_daily.queries import SegmentsQuery


def render() -> str:
    """Returns the rendered segments query, on one line."""
    query = SegmentsQuery(
        source_segments="project.dataset.segments",
        source_fragments="project.dataset.fragments",
        start_date=date(2024, 1, 1),
        end_date=date(2024, 1, 3),
    )
    return " ".join(query.render().split())


def test_reads_the_segments_and_the_fragments_of_the_date_range() -> None:
    """Checks that the query joins both tables and reads the days before the end date."""
    query = render()

    assert "FROM `project.dataset.segments` AS seg" in query
    assert "JOIN `project.dataset.fragments` AS frag USING (frag_id)" in query
    assert "DATE(seg.timestamp) >= '2024-01-01' AND DATE(seg.timestamp) < '2024-01-03'" in query
    assert "DATE(frag.timestamp) >= '2024-01-01' AND DATE(frag.timestamp) < '2024-01-03'" in query


def test_leaves_out_the_cumulative_columns() -> None:
    """Checks that the query does not read the cumulative identities and destinations."""
    assert "EXCEPT (cumulative_identities, cumulative_destinations)" in render()
