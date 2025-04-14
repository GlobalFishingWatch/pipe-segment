import functools
import logging
import typing

from google.cloud import bigquery
from google.cloud.exceptions import BadRequest, NotFound


class BigQueryTools():
    def __init__(self, project: str):
        self.client = bigquery.Client(project=project)

    def get_bq_table(self, dataset_id: str, table_id: str) -> bigquery.Table:
        """Returns the bigquery.Table from the dataset and table ids.
        :param dataset_id: The id of the dataset of the table.
        :param table_id: The id of the table."""
        dataset_ref = self.client.dataset(dataset_id)
        table_ref = dataset_ref.table(table_id)
        return self.client.get_table(table_ref)

    def remove_content(
        self,
        table: str,
        date_range: str,
        labels: typing.List[str] = [],
        partition_field: str = "timestamp"
    ) -> bool:
        """Removes the data content from a table to specific date range.
        Useful when we need to re-process a time period with fresh data.

        :param table: Table path Format: <dataset.table>.
        :param date_range: Date range to clean the table content.
        :param labels: Labels to audit the operation.
        :param partition_field: Partition field of the table.
        """
        logging.info(f'Removing the data content of [{table}] period <{date_range}>.')
        table_ds, table_tb = table.split('.')
        date_from, date_to = list(map(lambda x: x.strip(), date_range.split(',')))
        labels = functools.reduce(
            lambda x, y: dict(x, **{y.split('=')[0]: y.split('=')[1]}),
            labels,
            dict()
        )

        try:
            table_ref = self.get_bq_table(table_ds, table_tb)
            table = self.client.get_table(table_ref)  # API request
            logging.info(f'Removing the data content: Ensures the table [{table}] exists.')
            # deletes the content
            query_job = self.client.query(
                f"""
                   DELETE FROM `{table}`
                   WHERE date({partition_field}) >= '{date_from}'
                   AND date({partition_field}) <= '{date_to}'
                """,  # incusive range
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
            return True
        except NotFound:
            logging.warn(f'Removing the data content: Table {table} NOT FOUND. We can go on.')

        except BadRequest as err:
            logging.error(f'Removing the data content: Bad request received {err}.')

        return False

    def update_description(self, table: str, description: str):
        """Updates the description of a table.

        :param table: The table id.
        :param description: The description of the table.
        """
        table = self.get_bq_table(*table.split("."))
        table.description = description
        table_updated = self.client.update_table(table, ["description"])  # API request
        logging.info(f"Update description <{table_updated}>.")

    def update_labels(self, table: str, labels: typing.List):
        """Updates the labels of the table.

        :param table: The table id.
        :param labels: The labels of the table.
        """
        table = self.get_bq_table(*table.split("."))
        table.labels = {x.split('=')[0]: x.split('=')[1] for x in labels}
        table_updated = self.client.update_table(table, ["labels"])  # API request
        logging.info(f"Update labels <{table_updated}>.")
