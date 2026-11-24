from datetime import date

import apache_beam as beam

from apache_beam.testing.test_pipeline import TestPipeline

from pipe_segment.pipelines.messages_segmented.transforms.read_messages import ReadMessages


SOURCES = ["project1.dataset1.table1", "project2.dataset2.table2"]


class ReadFromBigQueryMock(beam.io.ReadFromBigQuery):
    def expand(self, pcoll):
        return pcoll | beam.Create(["dummy"])


def test_read_messages(monkeypatch):
    # TODO: replace this monkey patch when design allows for more easy testing.
    monkeypatch.setattr(beam.io, "ReadFromBigQuery", ReadFromBigQueryMock)

    # Test without ssvid_filter_query
    op = ReadMessages(SOURCES, date.today(), date.today(), {})
    with TestPipeline() as p:
        p | op

    # Test with ssvid_filter_query
    op = ReadMessages(SOURCES, date.today(), date.today(), {}, ssvid_filter_query="")
    with TestPipeline() as p:
        p | op


def test_read_messages_labels_every_read(monkeypatch):
    read_calls = []

    def fake_read(**kwargs):
        read_calls.append(kwargs)
        return beam.Create([])

    monkeypatch.setattr(beam.io, "ReadFromBigQuery", fake_read)

    labels = {"environment": "production", "stage": "segment--v5-0-4"}

    _ = beam.Pipeline() | ReadMessages(SOURCES, date.today(), date.today(), labels)

    assert [call["bigquery_job_labels"] for call in read_calls] == [labels, labels]


def test_read_messages_reads_a_query_per_source():
    op = ReadMessages(SOURCES, date(2024, 1, 1), date(2024, 1, 3), {})

    queries = [" ".join(op.build_query(source).split()) for source in SOURCES]

    assert "FROM `project1.dataset1.table1`" in queries[0]
    assert "FROM `project2.dataset2.table2`" in queries[1]
    assert all("BETWEEN '2024-01-01' AND '2024-01-03'" in query for query in queries)
