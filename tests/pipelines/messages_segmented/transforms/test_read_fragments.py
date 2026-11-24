from datetime import date

from google.cloud import bigquery
from google.api_core.exceptions import BadRequest, NotFound

import apache_beam as beam

from apache_beam.testing.test_pipeline import TestPipeline

from gfw.common.bigquery.helper import BigQueryHelper
from pipe_segment.pipelines.messages_segmented.transforms.read_fragments import ReadFragments


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


def make_bq_helper(notfound_table: bool) -> BigQueryHelper:
    """Returns a helper whose client finds the tables, or does not find any."""
    bq_helper = BigQueryHelper.mocked(project="test-project")

    def get_table(table_id):
        if notfound_table:
            raise NotFound("Not found")
        return bigquery.Table(table_id)

    bq_helper.client.get_table.side_effect = get_table
    return bq_helper


def test_read_fragments(monkeypatch):
    # TODO: replace this monkey patch when design allows for more easy testing.
    monkeypatch.setattr(beam.io, "ReadFromBigQuery", ReadFromBigQueryMock)

    bq_helper = make_bq_helper(notfound_table=True)
    dummy_source = "dummy_project.dummy_ds.dummy_table"

    # Test raising BadRequest not create if missing
    op = ReadFragments(bq_helper, dummy_source, date.today(), date.today(), {})
    with TestPipeline() as p:
        p | op

    # Test raising BadRequest and create if missing
    op = ReadFragments(bq_helper, dummy_source, date.today(), date.today(), {})
    with TestPipeline() as p:
        p | op

    bq_helper = make_bq_helper(notfound_table=False)
    # Test without raising BadRequest
    op = ReadFragments(bq_helper, dummy_source, date.today(), date.today(), {})
    with TestPipeline() as p:
        p | op

    # Test without start date
    op = ReadFragments(bq_helper, dummy_source, None, date.today(), {})
    with TestPipeline() as p:
        p | op

    # TODO: add asserts about outputs
