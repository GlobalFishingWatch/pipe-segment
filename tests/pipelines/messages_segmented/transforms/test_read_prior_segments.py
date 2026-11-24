from datetime import date

from pipe_segment.pipelines.messages_segmented.transforms.read_prior_segments import (
    ReadPriorSegments,
)


def make_transform(ssvid_filter_query=None, **kwargs):
    return ReadPriorSegments(
        "project.dataset.segments",
        "project.dataset.fragments",
        date(2024, 4, 11),
        ssvid_filter_query=ssvid_filter_query,
        **kwargs,
    )


def test_build_query_reads_the_day_from_both_tables():
    query = make_transform().build_query()

    assert "FROM `project.dataset.segments` AS s" in query
    assert "JOIN `project.dataset.fragments` AS f" in query
    assert "DATE(s.timestamp) = '2024-04-11'" in query
    assert "DATE(f.timestamp) = '2024-04-11'" in query
    assert "s.ssvid IN" not in query


def test_build_query_filters_ssvids():
    query = make_transform("SELECT ssvid FROM t").build_query()

    assert "AND s.ssvid IN (SELECT ssvid FROM t)" in query


def test_read_from_bigquery_kwargs_are_empty_by_default():
    assert make_transform().read_from_bigquery_kwargs == {}


def test_read_from_bigquery_kwargs_are_forwarded(mocker):
    read = mocker.patch("apache_beam.io.ReadFromBigQuery")
    labels = {"a": "b"}
    transform = make_transform(read_from_bigquery_kwargs={"bigquery_job_labels": labels})

    transform.expand(mocker.MagicMock())

    assert read.call_args.kwargs["bigquery_job_labels"] == labels
    assert read.call_args.kwargs["query"] == transform.build_query()
