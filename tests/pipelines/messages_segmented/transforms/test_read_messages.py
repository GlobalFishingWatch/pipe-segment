import os
from datetime import date

import pytest

from google.api_core.client_options import ClientOptions
from google.api_core.exceptions import NotFound
from google.auth.credentials import AnonymousCredentials
from google.cloud import bigquery

import apache_beam as beam
from apache_beam.testing.test_pipeline import TestPipeline
from pipe_segment.pipelines.messages_segmented.transforms.read_messages import ReadMessages
from gfw.common.bigquery.helper import BigQueryHelper

TABLE_SCHEMA = [
    {
        "description": "The Specific Source Vessel ID, in this case the MMSI",
        "mode": "NULLABLE",
        "name": "ssvid",
        "type": "STRING",
    },
    {
        "description": "The timestap that indicates when the message was transmitted.",
        "mode": "NULLABLE",
        "name": "timestamp",
        "type": "TIMESTAMP",
    },
]

INTEGRATION_TESTS = os.getenv("INTEGRATION_TESTS", None)
API_HOST = os.getenv("BIGQUERY_HOST", "localhost")
API_PORT = 9050


def make_bq_helper() -> BigQueryHelper:
    """Returns a helper whose client finds every table, partitioned by its timestamp."""
    bq_helper = BigQueryHelper.mocked(project="test-project")

    def get_table(table_id):
        table = bigquery.Table(table_id)
        table.time_partitioning = bigquery.table.TimePartitioning(field="timestamp")
        return table

    bq_helper.client.get_table.side_effect = get_table
    return bq_helper


class ReadFromBigQueryMock(beam.io.ReadFromBigQuery):
    def expand(self, pcoll):
        return pcoll | beam.Create(["dummy"])


def test_read_messages(monkeypatch):
    # TODO: replace this monkey patch when design allows for more easy testing.
    monkeypatch.setattr(beam.io, "ReadFromBigQuery", ReadFromBigQueryMock)

    dummy_sources = ["project1.dataset1.table1", "project2.dataset2.table2"]

    bq_helper = make_bq_helper()

    # Test without ssvid_filter_query
    op = ReadMessages(bq_helper, dummy_sources, date.today(), date.today(), {})
    with TestPipeline() as p:
        p | op

    # Test with ssvid_filter_query
    op = ReadMessages(
        bq_helper, dummy_sources, date.today(), date.today(), {}, ssvid_filter_query=""
    )
    with TestPipeline() as p:
        p | op


def test_read_messages_labels_every_read(monkeypatch):
    read_calls = []

    def fake_read(**kwargs):
        read_calls.append(kwargs)
        return beam.Create([])

    monkeypatch.setattr(beam.io, "ReadFromBigQuery", fake_read)

    labels = {"environment": "production", "stage": "segment--v5-0-4"}
    bq_helper = make_bq_helper()
    dummy_sources = ["project1.dataset1.table1", "project2.dataset2.table2"]

    _ = beam.Pipeline() | ReadMessages(
        bq_helper, dummy_sources, date.today(), date.today(), labels
    )

    assert [call["bigquery_job_labels"] for call in read_calls] == [labels, labels]


@pytest.mark.skipif(INTEGRATION_TESTS is None, reason="Integration tests disabled")
def test_bigquery_integration():
    # Must have a bigquery-emulator instance running.
    client_options = ClientOptions(api_endpoint=f"http://{API_HOST}:{API_PORT}")

    # These project and dataset created by the bigquery-emulator
    project_id = "test_project"
    dataset_id = "test_dataset"

    bq_client = bigquery.Client(
        project=project_id,
        client_options=client_options,
        credentials=AnonymousCredentials(),
    )

    bq_helper = BigQueryHelper(client_factory=lambda **kwargs: bq_client)

    # Partitioned table
    dummy_table1 = f"{project_id}.{dataset_id}.table1"
    table1 = bigquery.Table(table_ref=dummy_table1, schema=TABLE_SCHEMA)
    table1.time_partitioning = bigquery.table.TimePartitioning()
    table1.time_partitioning.field = 'timestamp'
    bq_client.create_table(table=table1, exists_ok=True)

    # Non existent table
    dummy_table2 = f"{project_id}.{dataset_id}.table2"

    start_date = date(2024, 1, 1)
    end_date = date(2024, 1, 5)

    dummy_sources = [dummy_table1, dummy_table2]
    op = ReadMessages(bq_helper, dummy_sources, start_date, end_date, {})  # noqa
    op.build_query(dummy_table1)

    with pytest.raises(NotFound):
        op.build_query(dummy_table2)
