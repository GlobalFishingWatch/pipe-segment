import functools
import json
import logging
import typing
import datetime as dt
from google.cloud import bigquery
from google.cloud.exceptions import BadRequest, NotFound
from time import sleep

logger = logging.getLogger(__name__)


DELETE_BETWEEN_DATES_QUERY = """
DELETE FROM `{table}` WHERE DATE({field}) >= '{start}' AND DATE({field}) <= '{end}'
"""


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

    # def create_or_clear_tables(self, tables, start_date, end_date=None, date_field="timestamp"):
    #     """Creates sharded tables. If they already exist, deletes their records.

    #     Args:
    #         tables: list of tables.
    #         start_date: the start of dates interval.
    #         end_date: the end of dates interval.
    #         date_field: name of the field with datetime.
    #     """
    #     logging.info("Ensure sharded tables are created for all dates...")
    #     if end_date is None or end_date == start_date:
    #         end_date = start_date + timedelta(days=1)

    #     for date in tools.list_of_days(start_date, end_date):
    #         for table_config in tables.values():
    #             table_ref, table_config = self._resolve_table_id(date, **table_config)
    #             table = self.create_table(table_ref, **table_config)
    #             self.clear_records(table, date_field, date, date)

    # def create_table(self, table_ref: str, **table_config) -> bigquery.Table:
    #     logging.info(f"Creating table {table_ref} (if it doesn't exists yet)...")
    #     table = self.build_table(table_ref=table_ref, **table_config)
    #     return self.client.create_table(table=table, exists_ok=True)

    # def clear_records(self, table, date_field, date_from, date_to):
    #     logging.info(
    #         "Deleting records at table {} between {} and {}..."
    #         .format(table.full_table_id, date_from, date_to)
    #     )

    #     full_table_id = table.full_table_id.replace(":", ".")

    #     query = DELETE_BETWEEN_DATES_QUERY.format(
    #         table=full_table_id, field=date_field, start=date_from, end=date_to)

    #     query_job = self.client.query(query, bigquery.QueryJobConfig(use_legacy_sql=False))

    #     return query_job.result()

    # def _resolve_table_id(self, date, table, **rest_of_config):
    #     if self.client.project not in table:
    #         table = f"{self.client.project}.{table}"

    #     table = f"{table}{date:%Y%m%d}"

    #     return table, rest_of_config

    # @staticmethod
    # def build_table(table_ref, schema, description):
    #     table = bigquery.Table(
    #         table_ref=table_ref.replace("bq://", "").replace(":", "."),
    #         schema=schema["fields"],
    #     )
    #     table.description = description

    #     return table
    def get_bq_table(self, dataset_id: str, table_id: str) -> bigquery.Table:
        """Returns the bigquery.Table from the dataset and table ids.

        Args:
            dataset_id: The id of the dataset of the table.
            table_id: The id of the table."""
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

        Args:
            table: Table path Format: <dataset.table>.
            date_range: Date range to clean the table content.
            labels: Labels to audit the operation.
            partition_field: Partition field of the table.
        """
        logger.info(f'Removing the data content of [{table}] period <{date_range}>.')
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
            logger.info(f'Removing the data content: Ensures the table [{table}] exists.')
            # deletes the content
            query_job = self.client.query(DELETE_BETWEEN_DATES_QUERY,
                bigquery.QueryJobConfig(
                    use_query_cache=False,
                    use_legacy_sql=False,
                    labels=labels,
                )
            )
            logger.info(f'Removing the data content: Job {query_job.job_id},'
                        f' is currently in state {query_job.state}')
            result = query_job.result()
            logger.info(f'Removing the data content: Date range '
                        f'[{date_from},{date_to}] cleaned in table {table}: {result}')
            return True
        except NotFound:
            logger.warn(f'Removing the data content: Table {table} NOT FOUND. We can go on.')

        except BadRequest as err:
            logger.error(f'Removing the data content: Bad request received {err}.')

        return False

    def update_description(self, table: str, description: str):
        """Updates the description of a table.

        Args:
            table: The table id.
            description: The description of the table.
        """
        table = self.get_bq_table(*table.split("."))
        table.description = description
        table_updated = self.client.update_table(table, ["description"])  # API request
        logger.info(f"Update description <{table_updated}>.")

    def update_labels(self, table: str, labels: typing.List):
        """Updates the labels of the table.

        Args:
            table: The table id.
            labels: The labels of the table.
        """
        table = self.get_bq_table(*table.split("."))
        table.labels = {x.split('=')[0]: x.split('=')[1] for x in labels}
        table_updated = self.client.update_table(table, ["labels"])  # API request
        logger.info(f"Update labels <{table_updated}>.")

    def update_table_schema(self, table, schema_file):
        """Updates the schema of the table.

        Args:
            table: The table id.
            schema: The schema of the table.
        """
        table = self.get_bq_table(*table.split("."))
        with open(schema_file) as file:
            table.schema = json.load(file)
        table_updated = self.client.update_table(table, ["schema"])  # API request
        logger.info(f"Update schema <{table_updated}>.")

    def table_ref(self, table: str) -> bigquery.TableReference:
        """Returns a table reference from a table id.

        Args:
            table: The table id."""
        return bigquery.TableReference.from_string(
            table, default_project=self.client.project
        )

    def _timeline_stats(self, timeline):
        stats = {
            "elapsed_ms": 0,
            "slot_millis": 0,
            "pending_units": "",
            "completed_units": "",
            "active_units": "",
        }
        if timeline:
            entry = timeline[-1]
            stats["elapsed_ms"] = int(entry.elapsed_ms)
            stats["slot_millis"] = int(entry.slot_millis)
            stats["pending_units"] = entry.pending_units
            stats["completed_units"] = entry.completed_units
            stats["active_units"] = entry.active_units
        return stats

    def _log_job_stats(self, job):
        execution_seconds = (job.ended - job.started).total_seconds()
        slot_seconds = (job.slot_millis or 0) / 1000
        logger.info(f"  execution_seconds:     {execution_seconds}")
        logger.info(f"  slot_seconds:          {slot_seconds}")
        logger.info(f"  num_child_jobs:        {job.num_child_jobs}")
        logger.info(f"  total_bytes_processed: {job.total_bytes_processed}")
        logger.info(f"  total_bytes_billed:    {job.total_bytes_billed}")
        logger.info("  referenced_tables:")
        for table in job.referenced_tables:
            logger.info(f"    {table.project}.{table.dataset_id}.{table.table_id}")
        if job.destination:
            logger.info("  output_table:")
            logger.info(f"    {job.destination}")

        logger.info(f"  reservation_usage:     {job.reservation_usage}")
        logger.info(f"  script_statistics:     {job.script_statistics}")

    def run_query(
        self,
        query,
        dest_table=None,
        write_disposition="WRITE_APPEND",
        partition_field=None,
        clustering_fields=None,
        job_priority="INTERACTIVE",
        labels={},
    ):
        """Run the query using the Bigquery client.

        Args:
            query: The sql query.
            dest_table: The destination table.
            write_disposition: The write disposition to use.
            partition_field: The partition field of the destination table.
            clustering_fields: The clustering fields of the destination table.
            job_priority: The priority of the QUERY JOB.
            labels: The labels to audit.
        """
        if dest_table:
            time_partitioning = None
            if partition_field is not None:
                time_partitioning = bigquery.table.TimePartitioning(
                    type_=bigquery.table.TimePartitioningType.MONTH,
                    field=partition_field,
                )

            config = bigquery.QueryJobConfig(
                destination=self.table_ref(dest_table),
                priority=job_priority,
                write_disposition=write_disposition,
                time_partitioning=time_partitioning,
                clustering_fields=clustering_fields,
                labels=labels,
            )
        else:
            config = bigquery.QueryJobConfig(
                priority=job_priority,
                labels=labels,
            )
        job = self.client.query(query, job_config=config)
        logger.info(f"Bigquery job created: {job.created:%Y-%m-%d %H:%M:%S}")
        logger.info(f"job_id: {job.job_id}")
        logger.debug("Running...")
        start_time = dt.datetime.now()
        i = 0
        while job.running():
            stats = self._timeline_stats(job.timeline)
            stats["run_time"] = (
                round((dt.datetime.now() - start_time).total_seconds()) + 1
            )
            stats["exec_s"] = round(stats["elapsed_ms"] / 1000)
            stats["slot_s"] = round(stats["slot_millis"] / 1000)
            if i % 10 == 0:
                line = (
                    "  elapsed: {run_time}s "
                    " exec: {exec_s}s  slot: {slot_s}s"
                    " pend: {pending_units} compl: {completed_units} active: {active_units}"
                    .format(
                        **stats
                    )
                )
                line = f"{line[:80]: <80}"
                if logger.level == logging.DEBUG:
                    logger.debug(line)

            i += 1
            sleep(0.1)
        if logger.level == logging.DEBUG:
            logger.debug(" " * 80)  # overwrite the previous line
            self.dump_query(job.query)
        logger.info("Bigquery job done.")

        if job.error_result:
            err = job.error_result["reason"]
            msg = job.error_result["message"]
            raise RuntimeError(f"{err}: {msg}")
        else:
            self._log_job_stats(job)
        return job.result()
