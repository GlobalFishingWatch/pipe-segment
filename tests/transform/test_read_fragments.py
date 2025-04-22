from datetime import date
from dataclasses import dataclass

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

@dataclass
class BigQueryClientMock:
    project: str

    def dataset(self, id: str):
        return DatasetMock()

    def query(self, query):
        if self.project is None:
            return ResultErrorMock()

        return ResultMock()

    def get_table(self, id: str):
        return id


class ReadFromBigQueryMock(beam.io.ReadFromBigQuery):
    def expand(self, pcoll):
        return pcoll | beam.Create([])


def test_read_fragments(monkeypatch):
    # TODO: replace this monkey patch when design allows for more easy testing.
    monkeypatch.setattr(bigquery, "Client", BigQueryClientMock)
    monkeypatch.setattr(beam.io, "ReadFromBigQuery", ReadFromBigQueryMock)

    dummy_source = "dummy_ds.dummy_table"

    # Test raising BadRequest not create if missing
    op = ReadFragments(dummy_source, date.today(), date.today())
    with TestPipeline() as p:
        p | op

    # Test raising BadRequest and create if missing
    op = ReadFragments(dummy_source, date.today(), date.today())
    with TestPipeline() as p:
        p | op

    # Test without raising BadRequest
    op = ReadFragments(dummy_source, date.today(), date.today(), project="dummy")
    with TestPipeline() as p:
        p | op

    # Test without start date
    op = ReadFragments(dummy_source, None, date.today(), project="dummy")
    with TestPipeline() as p:
        p | op

    # TODO: add asserts about outputs
