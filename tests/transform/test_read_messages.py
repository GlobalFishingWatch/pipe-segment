from datetime import date
from unittest import mock
from dataclasses import dataclass

import apache_beam as beam
from google.cloud import bigquery

from apache_beam.testing.test_pipeline import TestPipeline

from pipe_segment.transform.read_messages import ReadMessages
from pipe_segment.models.bigquery_message_source import BigQueryMessagesSource
from pipe_segment.utils.bqtools import BigQueryTools


@dataclass
class BigQueryMessagesSourceMock(BigQueryMessagesSource):
    qualified_source_messages: str = ""

    def filter_messages(self, start_date, end_date):
        return ""


class ReadFromBigQueryMock(beam.io.ReadFromBigQuery):
    def expand(self, pcoll):
        return pcoll | beam.Create(["dummy"])


def test_read_messages(monkeypatch):
    # TODO: replace this monkey patch when design allows for more easy testing.
    monkeypatch.setattr(beam.io, "ReadFromBigQuery", ReadFromBigQueryMock)

    table = "project.dataset.table"
    dummy_sources = [table, table]

    client = mock.create_autospec(bigquery.Client, instance=True)
    bqtools = BigQueryTools(client)

    # Test without ssvid_filter_query
    op = ReadMessages(bqtools, dummy_sources, start_date=date.today(), end_date=date.today())
    with TestPipeline() as p:
        p | op

    # Test with ssvid_filter_query
    op = ReadMessages(
        bqtools,
        dummy_sources,  start_date=date.today(), end_date=date.today(), ssvid_filter_query="")

    with TestPipeline() as p:
        p | op
