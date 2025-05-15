import os
from datetime import datetime, timedelta

import pytest

from google.api_core.client_options import ClientOptions
from google.auth.credentials import AnonymousCredentials
from google.cloud import bigquery

from pipe_segment.utils.bq_tools import BigQueryTools

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

TABLE_NAME = "test_dataset.test_table"
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

    table = bigquery.Table(TABLE_FULL_ID)
    table.schema = TABLE_SCHEMA["fields"]
    table.description = TABLE_DESCRIPTION
    client.create_table(table, exists_ok=True)

    assert isinstance(table, bigquery.Table)
    assert bqtools.get_bq_table(*TABLE_NAME.split("."))  # table exists.
    assert len(list(client.list_rows(TABLE_NAME))) == 0

    start_date = datetime(2024, 1, 1).date()
    end_date = start_date
    date_range = f"{start_date.isoformat()},{end_date.isoformat()}"
    bqtools.remove_content(DESTINATION_TABLES["messages"]["table"], date_range)

    end_date = start_date + timedelta(days=1)
    date_range = f"{start_date.isoformat()},{end_date.isoformat()}"
    bqtools.remove_content(DESTINATION_TABLES["messages"]["table"], date_range)

    bqtools = BigQueryTools.build(
        project=PROJECT, client_options=client_options, credentials=AnonymousCredentials())
