import logging, functools
from google.cloud import bigquery
from google.cloud.exceptions import NotFound
from datetime import timedelta


def build_sink_table_descriptor(table_id, schema, description):
    table = bigquery.Table(
        table_id.replace("bq://", "").replace(":", "."),
        schema=schema,
    )
    table.description = description
    return table


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

    def ensure_table_exists(
        self,
        table_path: str,
        schema: dict,
        partition_field: str,
        description: str,
        clustering_fields: list = [],
        labels: list = [],
        partition_filter: bool = False,
    ) -> bigquery.Table:
        """Ensure the table exist, if it does not exist the table is created
        if not returns the table.
        """
        logging.info(f"Ensuring table {table_path} exists")
        table_ds, table_tb = table_path.split(".")
        try:
            table = self.get_bq_table(table_ds, table_tb)
        except NotFound as err:
            ds_ref = bigquery.DatasetReference(self.client.project, table_ds)
            tb_ref = ds_ref.table(table_tb)
            table = bigquery.Table(tb_ref, schema=schema)
            table.time_partitioning = bigquery.TimePartitioning(
                type_ = bigquery.TimePartitioningType.MONTH,
                field = partition_field,
            )
            table.require_partition_filter = partition_filter
            clustering_fields.insert(0, partition_field)
            table.clustering_fields = clustering_fields
            table.description = description
            table.labels = {x.split('=')[0]: x.split('=')[1] for x in labels}
            table = self.client.create_table(table)

        logging.info(f"Table {table.full_table_id} exists")
        return table

    def remove_content(
        self,
        table: str,
        date_range: str,
        labels: list = [],
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
        date_from, date_to = list(map(lambda x:x.strip(), date_range.split(',')))
        labels = functools.reduce(
            lambda x,y: dict(x, **{y.split('=')[0]: y.split('=')[1]}),
            labels,
            dict()
        )

        try:
            table_ref = self.get_bq_table(table_ds, table_tb)
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
            return True
        except NotFound as nferr:
            logging.warn(f'Removing the data content: Table {table} NOT FOUND. We can go on.')

        except BadRequest as err:
            logging.error(f'Removing the data content: Bad request received {err}.')

        return False
