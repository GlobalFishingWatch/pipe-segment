from datetime import date

import apache_beam as beam

from apache_beam.testing.test_pipeline import TestPipeline

from pipe_segment.pipelines.messages_segmented.transforms.read_messages import ReadMessages


SOURCE = "project1.dataset1.table1"


class ReadFromBigQueryMock(beam.io.ReadFromBigQuery):
    def expand(self, pcoll):
        return pcoll | beam.Create(["dummy"])


def test_read_messages(monkeypatch):
    # TODO: replace this monkey patch when design allows for more easy testing.
    monkeypatch.setattr(beam.io, "ReadFromBigQuery", ReadFromBigQueryMock)

    # Test without ssvid_filter_query
    op = ReadMessages(SOURCE, date.today(), date.today(), {})
    with TestPipeline() as p:
        p | op

    # Test with ssvid_filter_query
    op = ReadMessages(SOURCE, date.today(), date.today(), {}, ssvid_filter_query="")
    with TestPipeline() as p:
        p | op


def test_read_messages_labels_the_read(monkeypatch):
    read_calls = []

    class RecordingRead(ReadFromBigQueryMock):
        def __init__(self, **kwargs):
            read_calls.append(kwargs)
            super().__init__(**kwargs)

    monkeypatch.setattr(beam.io, "ReadFromBigQuery", RecordingRead)

    labels = {"environment": "production", "stage": "segment--v5-0-4"}

    _ = beam.Pipeline() | ReadMessages(SOURCE, date.today(), date.today(), labels)

    assert [call["bigquery_job_labels"] for call in read_calls] == [labels]


def test_read_messages_reads_the_query_of_its_source():
    op = ReadMessages(SOURCE, date(2024, 1, 1), date(2024, 1, 3), {})

    query = " ".join(op.build_query().split())

    assert "FROM `project1.dataset1.table1`" in query
    assert "BETWEEN '2024-01-01' AND '2024-01-03'" in query
