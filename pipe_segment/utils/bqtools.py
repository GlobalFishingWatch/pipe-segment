import logging
from google.cloud import bigquery

from pipe_segment import tools


def build_sink_table_descriptor(table_id, schema, description):
    table = bigquery.Table(
        table_id.replace("bq://", "").replace(":", "."),
        schema=schema,
    )
    table.description = description
    return table


class BigQueryTools:
    def __init__(self, project):
        self.client = bigquery.Client(project=project)

    def create_table(self, table):
        logging.info(f"Creating table {table.table_id} (if it doesn't exists yet)...")
        result = self.client.create_table(table=table, exists_ok=True)

        return result

    def clear_records(self, table, date_field, date_from, date_to):
        logging.info(
            f"""Deleting records at
            table {table.full_table_id} between {date_from:%Y-%m-%d} and {date_to:%Y-%m-%d}""")
        query_job = self.client.query(
            f"""
               DELETE FROM `{table.project}.{table.dataset_id}.{table.table_id}`
               WHERE DATE({date_field}) BETWEEN '{date_from:%Y-%m-%d}' AND '{date_to:%Y-%m-%d}'
            """,
            bigquery.QueryJobConfig(
                use_legacy_sql=False,
            )
        )
        result = query_job.result()

        logging.info(
            f"""Records at table {table.full_table_id} between
            {date_from:%Y-%m-%d} and {date_to:%Y-%m-%d} deleted""")

        return result

    def ensure_sharded_tables_creation(
        self,
        start_date,
        end_date,
        destination_tables,
        key="timestamp"
    ):
        # Ensure sharded tables are created for all dates
        logging.info("Ensure sharded tables are created for all dates...")

        def full_tbl_id(tbl): return f"{self.client.project}.{tbl}" if not (
            self.client.project in tbl) else tbl

        for dt in tools.list_of_days(start_date, end_date):

            for dst in destination_tables.keys():
                sink_table_descriptor = build_sink_table_descriptor(
                    table_id=f"{full_tbl_id(destination_tables[dst]['table'])}{dt:%Y%m%d}",
                    schema=destination_tables[dst]["schema"]["fields"],
                    description=destination_tables[dst]["description"]
                )
                sink_table = self.create_table(sink_table_descriptor)
                self.clear_records(sink_table, key, dt, dt)
