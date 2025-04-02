import logging, functools
from datetime import timedelta

from google.cloud import bigquery


def build_sink_table_descriptor(table_id, schema, description):
    table = bigquery.Table(
        table_id.replace("bq://", "").replace(":", "."),
        schema=schema,
    )
    table.description = description
    return table


class BigQueryTools():
    def __init__(self, project):
        self.client = bigquery.Client(project=project)

    def get_bq_table(self, dataset_id, table_id):
        dataset_ref = self.client.dataset(dataset_id)
        table_ref = dataset_ref.table(table_id)
        return self.client.get_table(table_ref)

    def ensure_table_exists(self, table):
        logging.info(f"Ensuring table {table.table_id} exists")
        result = self.client.create_table(table=table, exists_ok=True)
        logging.info(f"Table {result.full_table_id} exists")
        return result

    def remove_content(
            table: str,
            date_range: str,
            labels: list = [],
            partition_field: str = "timestamp"
    ):
        """Removes the data content from a table to specific date range.
        Useful when we need to re-process a time period with fresh data.

        :param table: Table path Format: <dataset.table>.
        :param date_range: Date range to clean the table content.
        :param labels: Labels to audit the operation.
        :param partition_field: Partition field of the table.
        """
        logging.info(f'Removing the data content of [{table}] period <{date_range}>.')
        table_ds, table_tb = table.split('.')
        table_ref = self.get_bq_table(table_ds, table_tb)
        date_from, date_to = list(map(lambda x:x.strip(), date_range.split(',')))
        labels = functools.reduce(
            lambda x,y: dict(x, **{y.split('=')[0]: y.split('=')[1]}),
            labels,
            dict()
        )

        try:
            table = self.client.get_table(table_ref) #API request
            logging.info(f'Removing the data content: Ensures the table [{table}] exists.')
            #deletes the content
            query_job = self.client.query(
                f"""
                   DELETE FROM `{table}`
                   WHERE date({partition_field}) >= '{date_from}'
                   AND date({partition_field}) <= '{date_to}'
                """, # incusive range
                bigquery.QueryJobConfig(
                    use_query_cache=False,
                    use_legacy_sql=False,
                    labels=labels,
                )
            )
            logging.info(f'Removing the data content: Job {query_job.job_id},'
                         f' is currently in state {query_job.state}')
            result = query_job.result()
            logging.info(f'Removing the data content: Date range '
                         f'[{date_from},{date_to}] cleaned in table {table}: {result}')

        except NotFound as nferr:
            logging.warn(f'Removing the data content: Table {table} NOT FOUND. We can go on.')

        except BadRequest as err:
            logging.error(f'Removing the data content: Bad request received {err}.')

        return result
