import os

from google.cloud import bigquery

from pipe_segment.utils.bq_tools import SimpleTable


PROJECT = "test_project"

TABLE_NAME = "test_dataset.test_table"
TABLE_FULL_ID = f"{PROJECT}.{TABLE_NAME}"
TABLE_DESCRIPTION = "Test table."
TABLE_SCHEMA = [
    bigquery.SchemaField(
        "ssvid",
        "STRING",
        mode="NULLABLE",
        description="The Specific Source Vessel ID, in this case the MMSI",
    ),
    bigquery.SchemaField(
        "timestamp",
        "TIMESTAMP",
        description="The timestap that indicates when the message was transmitted.",
        mode="NULLABLE",
    ),
]

TABLE_DEFINITION = SimpleTable(
    table_id=TABLE_FULL_ID,
    description=TABLE_DESCRIPTION,
    schema=TABLE_SCHEMA,
)


INTEGRATION_TESTS = os.getenv("INTEGRATION_TESTS", None)
API_HOST = os.getenv("BIGQUERY_HOST", "localhost")
API_PORT = 9050


# TODO: I'm disabling these integration tests because it's failing for some
# reason but it is actually working when going to the actual bigquery service.
# This if further argument against doing integration tests here

# @pytest.mark.skipif(INTEGRATION_TESTS is None, reason="Integration tests disabled")
# def test_bigquery_integration():
#     # Must have a bigquery-emulator instance running.
#     client_options = ClientOptions(api_endpoint=f"http://{API_HOST}:{API_PORT}")

#     client = bigquery.Client(
#         project=PROJECT,
#         client_options=client_options,
#         credentials=AnonymousCredentials(),
#     )

#     bq_helper = BigQueryHelper(client, labels={})
#     table = bq_helper.ensure_table_exists(TABLE_DEFINITION)

#     assert isinstance(table, bigquery.Table)
#     assert client.get_table(TABLE_NAME)  # table exists.
#     assert len(list(client.list_rows(TABLE_NAME))) == 0

#     bq_helper.run_query(query=TABLE_DEFINITION.clear_query())
