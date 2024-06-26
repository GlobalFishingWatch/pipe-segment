import os
from datetime import datetime, timedelta

import pytest

from google.api_core.client_options import ClientOptions
from google.auth.credentials import AnonymousCredentials
from google.cloud import bigquery

from pipe_segment.utils.bqtools import BigQueryTools

TABLE_SCHEMA = {
    "fields": [
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
}

TABLE_NAME = "test_dataset.test_table_"
TABLE_DESCRIPTION = "Test table."

DESTINATION_TABLES = dict(
    messages={
        "table": TABLE_NAME,
        "schema": TABLE_SCHEMA,
        "description": TABLE_DESCRIPTION
    }
)

PROJECT = "test_project"

TABLE_FULL_ID = f"{PROJECT}.{TABLE_NAME}"

INTEGRATION_TESTS = os.getenv("INTEGRATION_TESTS", None)
API_HOST = os.getenv("BIGQUERY_HOST", "localhost")
API_PORT = 9050


@pytest.mark.skipif(INTEGRATION_TESTS is None, reason="Integration tests disabled")
def test_bigquery_integration():
    # Must have a bigquery-emulator instance running.
    client_options = ClientOptions(api_endpoint=f"http://{API_HOST}:{API_PORT}")

    client = bigquery.Client(
        project="test_project",
        client_options=client_options,
        credentials=AnonymousCredentials(),
    )

    bqtools = BigQueryTools(client)

    table = bqtools.create_table(
        table_ref=TABLE_FULL_ID,
        schema=TABLE_SCHEMA,
        description=TABLE_DESCRIPTION
    )

    assert isinstance(table, bigquery.Table)
    dataset, table = TABLE_NAME.split(".")
    assert bqtools.get_table(dataset, table)  # table exists.
    assert len(list(client.list_rows(TABLE_NAME))) == 0

    start_date = datetime(2024, 1, 1).date()
    end_date = start_date
    bqtools.create_or_clear_tables(DESTINATION_TABLES, start_date, end_date)

    end_date = start_date + timedelta(days=1)
    bqtools.create_or_clear_tables(DESTINATION_TABLES, start_date, end_date)

    bqtools = BigQueryTools.build(
        project=PROJECT, client_options=client_options, credentials=AnonymousCredentials())
