from datetime import date
from dataclasses import dataclass, field

from google.cloud import bigquery
from google.api_core.exceptions import BadRequest

import apache_beam as beam

from apache_beam.testing.test_pipeline import TestPipeline

from pipe_segment.transform.read_fragments import ReadFragments


class RowMock:
    min_suffix = "20240301"


class ResultMock:
    def result(self):
        return [RowMock()]


class ResultErrorMock:
    def result(self):
        raise BadRequest("Bad request")


class DatasetMock:
    def table(self, id: str):
        return id


class ReadFromBigQueryMock(beam.io.ReadFromBigQuery):
    def expand(self, pcoll):
        return pcoll | beam.Create([])


@dataclass
class BigQueryHelperMock():
    notfound_table: bool = True
    labels: dict = field(default_factory=lambda: {})

    def fetch_table(self, table_ref) -> bigquery.Table:
        return None if self.notfound_table else bigquery.Table(table_ref)


def test_read_fragments(monkeypatch):
    # TODO: replace this monkey patch when design allows for more easy testing.
    monkeypatch.setattr(beam.io, "ReadFromBigQuery", ReadFromBigQueryMock)

    bq_helper = BigQueryHelperMock(notfound_table=True)
    dummy_source = "dummy_project.dummy_ds.dummy_table"

    # Test raising BadRequest not create if missing
    op = ReadFragments(bq_helper, dummy_source, date.today(), date.today())
    with TestPipeline() as p:
        p | op

    # Test raising BadRequest and create if missing
    op = ReadFragments(bq_helper, dummy_source, date.today(), date.today())
    with TestPipeline() as p:
        p | op

    bq_helper = BigQueryHelperMock(notfound_table=False)
    # Test without raising BadRequest
    op = ReadFragments(bq_helper, dummy_source, date.today(), date.today())
    with TestPipeline() as p:
        p | op

    # Test without start date
    op = ReadFragments(bq_helper, dummy_source, None, date.today())
    with TestPipeline() as p:
        p | op

    # TODO: add asserts about outputs
