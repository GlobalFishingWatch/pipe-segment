from datetime import date

import apache_beam as beam

from apache_beam.testing.test_pipeline import TestPipeline

from pipe_segment.pipelines.messages_segmented.transforms.read_fragments import ReadFragments


class ReadFromBigQueryMock(beam.io.ReadFromBigQuery):
    def expand(self, pcoll):
        return pcoll | beam.Create([])


SOURCE = "dummy_project.dummy_ds.dummy_table"


def test_read_fragments(monkeypatch):
    # TODO: replace this monkey patch when design allows for more easy testing.
    monkeypatch.setattr(beam.io, "ReadFromBigQuery", ReadFromBigQueryMock)

    op = ReadFragments(SOURCE, date.today(), date.today(), {})
    with TestPipeline() as p:
        p | op

    # Test without start date
    op = ReadFragments(SOURCE, None, date.today(), {})
    with TestPipeline() as p:
        p | op

    # TODO: add asserts about outputs


def test_the_query_reads_the_days_up_to_the_end_date():
    query = ReadFragments(SOURCE, date(2024, 1, 1), date(2024, 1, 3), {}).make_query()

    assert f"FROM `{SOURCE}`" in query
    assert "date(timestamp) >= 2024-01-01 AND date(timestamp) <= 2024-01-03" in query


def test_the_query_reads_every_day_without_a_start_date():
    query = ReadFragments(SOURCE, None, date(2024, 1, 3), {}).make_query()

    assert "WHERE" not in query


def test_the_job_is_labeled(monkeypatch):
    read_calls = []

    class RecordingRead(ReadFromBigQueryMock):
        def __init__(self, **kwargs):
            read_calls.append(kwargs)
            super().__init__(**kwargs)

    monkeypatch.setattr(beam.io, "ReadFromBigQuery", RecordingRead)
    labels = {"environment": "test"}

    _ = beam.Pipeline() | ReadFragments(SOURCE, None, date(2024, 1, 3), labels)

    assert [call["bigquery_job_labels"] for call in read_calls] == [labels]
