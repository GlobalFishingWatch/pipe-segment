import logging

from google.cloud import bigquery
from google.api_core import exceptions

logger = logging.getLogger(__name__)


class BigQuerySource:
    SHARDED_SUFFIX = "*"

    def __init__(
        self,
        bqclient: bigquery.Client,
        table_id: str,
    ):
        self._bqclient = bqclient
        self.table_id = table_id.replace("bq://", "").replace(":", ".")

        # Default values (partitioned)
        self.filtering_field = "timestamp"
        self.date_format = "%Y-%m-%d"
        self.qualified_source = self.table_id

        self._check_sharded_or_partitioned()

    def _check_sharded_or_partitioned(self):
        # TODO: simplify this. Maybe directly on ReadMessages transform...
        try:
            table = self._bqclient.get_table(self.table_id)
            if table.time_partitioning:
                self.filtering_field = table.time_partitioning.field

            logging.info(f'Table {self.table_id} is partitioned on field {self.filtering_field}')
            return table
        except exceptions.NotFound:
            logger.info(f"Table {self.table_id} not found as partitioned.")

        try:
            logger.info("Checking if it's a sharded table...")
            self.qualified_source = f"{self.table_id}{self.SHARDED_SUFFIX}"
            self._bqclient.get_table(self.qualified_source)
            logging.info(f'Table {self.table_id} is date sharded')

            self.filtering_field = "_TABLE_SUFFIX"
            self.date_format = "%Y%m%d"
        except exceptions.NotFound:
            raise ValueError(
                f'Table {self.table_id} could not be found as partitioned or sharded.')
