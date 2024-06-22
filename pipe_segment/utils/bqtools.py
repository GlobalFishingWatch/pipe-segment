import logging
from datetime import timedelta


from google.cloud import bigquery

from pipe_segment import tools


DELETE_BETWEEN_DATES_QUERY = "DELETE FROM `{table}` WHERE DATE({field}) BETWEEN '{start}' AND '{end}'"


class BigQueryTools:
    """Wrapper above bigquery library with extra features.

    Args:
        client: a bigquery client.
    """
    def __init__(self, client: bigquery.Client):
        self.client = client

    @classmethod
    def build(cls, project: str = None, **kwargs):
        """Builds a BigQueryTools objec.

        Args:
            project: the name of the project.
            kwargs: any parameter allowed by bigquery.Client constructor.
                https://cloud.google.com/python/docs/reference/bigquery/latest/google.cloud.bigquery.client.Client
        """
        return cls(bigquery.Client(project=project, **kwargs))

    def create_or_clear_tables(self, tables, start_date, end_date=None, date_field="timestamp"):
        """Creates sharded tables. If they already exist, deletes their records.

        Args:
            tables: list of tables.
            start_date: the start of dates interval.
            end_date: the end of dates interval.
            date_field: name of the field with datetime.
        """
        logging.info("Ensure sharded tables are created for all dates...")
        if end_date is None or end_date == start_date:
            end_date = start_date + timedelta(days=1)

        for date in tools.list_of_days(start_date, end_date):
            for table_config in tables.values():
                table_ref, table_config = self._resolve_table_id(date, **table_config)
                table = self.create_table(table_ref, **table_config)
                self.clear_records(table, date_field, date, date)

    def create_table(self, table_ref: str, **table_config) -> bigquery.Table:
        logging.info(f"Creating table {table_ref} (if it doesn't exists yet)...")
        table = self.build_table(table_ref=table_ref, **table_config)
        return self.client.create_table(table=table, exists_ok=True)

    def clear_records(self, table, date_field, date_from, date_to):
        logging.info(
            "Deleting records at table {} between {} and {}..."
            .format(table.full_table_id, date_from, date_to)
        )

        full_table_id = table.full_table_id.replace(":", ".")

        query = DELETE_BETWEEN_DATES_QUERY.format(
            table=full_table_id, field=date_field, start=date_from, end=date_to)

        query_job = self.client.query(query, bigquery.QueryJobConfig(use_legacy_sql=False))

        return query_job.result()

    def _resolve_table_id(self, date, table, **rest_of_config):
        if self.client.project not in table:
            table = f"{self.client.project}.{table}"

        table = f"{table}{date:%Y%m%d}"

        return table, rest_of_config

    @staticmethod
    def build_table(table_ref, schema, description):
        table = bigquery.Table(
            table_ref=table_ref.replace("bq://", "").replace(":", "."),
            schema=schema["fields"],
        )
        table.description = description

        return table
