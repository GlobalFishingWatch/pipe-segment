import logging
from google.cloud import bigquery

class BigQueryTools():
    def __init__(self, project):
        self.client = bigquery.Client(project=project)

    def ensure_table_exists(self, table):
        logging.info(f'Ensuring table {table.table_id} exists')
        result = self.client.create_table(table=table, exists_ok=True)
        logging.info(f'Table {result.full_table_id} exists')
        return result

    def clear_records(self, table, date_field, date_from, date_to):
        logging.info(f'Deleting records at table {table.full_table_id} between {date_from:%Y-%m-%d} and {date_to:%Y-%m-%d}')
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
        logging.info(f'Records at table {table.full_table_id} between {date_from:%Y-%m-%d} and {date_to:%Y-%m-%d} deleted')
        return result


