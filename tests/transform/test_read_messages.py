import os
from datetime import date
from dataclasses import dataclass

import pytest

from google.api_core import exceptions
from google.api_core.client_options import ClientOptions
from google.auth.credentials import AnonymousCredentials
from google.cloud import bigquery

import apache_beam as beam
from apache_beam.testing.test_pipeline import TestPipeline
from pipe_segment.transform.read_messages import ReadMessages
from pipe_segment import tools

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


@dataclass
class BigQueryClientMock():
    project: str
    notfound_fails: int = 0

    def get_table(self, table_ref) -> bigquery.Table:
        if self.notfound_fails > 0:
            self.notfound_fails -= 1
            raise exceptions.NotFound("Table not found.")

        table = bigquery.Table(table_ref)
        return table


class ReadFromBigQueryMock(beam.io.ReadFromBigQuery):
    def expand(self, pcoll):
        return pcoll | beam.Create(["dummy"])


def test_read_messages(monkeypatch):
    # TODO: replace this monkey patch when design allows for more easy testing.
    monkeypatch.setattr(beam.io, "ReadFromBigQuery", ReadFromBigQueryMock)

    dummy_sources = ["project1.dataset1.table1", "project2.dataset2.table2"]

    bq_client = BigQueryClientMock(project="test", notfound_fails=1)

    # Test without ssvid_filter_query
    op = ReadMessages(bq_client, dummy_sources, date.today(), date.today())
    with TestPipeline() as p:
        p | op

    # Test with ssvid_filter_query
    op = ReadMessages(bq_client, dummy_sources, date.today(), date.today(), ssvid_filter_query="")
    with TestPipeline() as p:
        p | op


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

    # Partitioned table
    dummy_table1 = f"{project_id}.{dataset_id}.table1"
    table1 = bigquery.Table(table_ref=dummy_table1, schema=TABLE_SCHEMA)
    table1.time_partitioning = bigquery.table.TimePartitioning()
    table1.time_partitioning.field = 'timestamp'
    bq_client.create_table(table=table1, exists_ok=True)

    # Non existent table
    dummy_table2 = f"{project_id}.{dataset_id}.table2"

    # How to create sharded table?
    dummy_table3 = f"{project_id}.{dataset_id}.table3_"

    start_date = date(2024, 1, 1)
    end_date = date(2024, 1, 5)

    days = tools.list_of_days(start_date, end_date)
    shard_date_format = "%Y%m%d"
    for day in days:
        date_str = day.strftime(shard_date_format)
        table3 = bigquery.Table(table_ref=f"{dummy_table3}{date_str}", schema=TABLE_SCHEMA)
        bq_client.create_table(table=table3, exists_ok=True)

    tables = bq_client.list_tables(dataset_id)
    print("Tables contained in '{}':".format(dataset_id))
    for table in tables:
        print("{}.{}.{}".format(table.project, table.dataset_id, table.table_id))

    dummy_sources = [dummy_table1, dummy_table2, dummy_table3]
    op = ReadMessages(bq_client, dummy_sources, start_date, end_date)  # noqa
    op.build_query(dummy_table1)

    with pytest.raises(ValueError):
        op.build_query(dummy_table2)

    # Querying the sharded tables with wildcard not working...Problem with bigquery-emulator?
    # wildcard_table = f"{dummy_table3}*"
    # table = bq_client.get_table(f"{dummy_table3}20240101")
    # op.build_query(dummy_table3)
