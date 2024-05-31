import logging
from google.cloud import bigquery
from datetime import timedelta

def build_sink_table_descriptor(table_id, schema, description):
    table = bigquery.Table(
        table_id.replace("bq://","").replace(":", "."),
        schema=schema,
    )
    table.description = description
    return table

class BigQueryTools():
    def __init__(self, project):
        self.client = bigquery.Client(project=project)

    def ensure_table_exists(self, table):
        logging.info(f"Ensuring table {table.table_id} exists")
        result = self.client.create_table(table=table, exists_ok=True)
        logging.info(f"Table {result.full_table_id} exists")
        return result

    def clear_records(self, table, date_field, date_from, date_to):
        logging.info(f"Deleting records at table {table.full_table_id} between {date_from:%Y-%m-%d} and {date_to:%Y-%m-%d}")
        query_job = self.client.query(
            f"""
               DELETE FROM `{table.project}.{table.dataset_id}.{table.table_id}`
               WHERE {date_field} BETWEEN '{date_from:%Y-%m-%d}' AND '{date_to:%Y-%m-%d}'
            """,
            bigquery.QueryJobConfig(
                use_legacy_sql=False,
            )
        )
        result = query_job.result()
        logging.info(f"Records at table {table.full_table_id} between {date_from:%Y-%m-%d} and {date_to:%Y-%m-%d} deleted")
        return result

    def ensure_sharded_tables_creation(self, start_date, end_date, destination_tables, key="timestamp"):
        # Ensure sharded tables are created for all dates
        logging.info("Ensure sharded tables are created for all dates")
        full_tbl_id = lambda tbl: f"{self.client.project}.{tbl}" if not (self.client.project in tbl) else tbl

        # Create list of days between start_date and end_date including the start_date.
        days = ([start_date]+[start_date+timedelta(days=x) for x in range((end_date-start_date).days)])
        for dt in days:

            for dst in destination_tables.keys():
                sink_table_descriptor = build_sink_table_descriptor(
                    table_id=f"{full_tbl_id(destination_tables[dst]['table'])}{dt:%Y%m%d}",
                    schema=destination_tables[dst]["schema"]["fields"],
                    description=destination_tables[dst]["description"]
                )
                sink_table = self.ensure_table_exists(sink_table_descriptor)
                self.clear_records(sink_table, key, dt, dt)

