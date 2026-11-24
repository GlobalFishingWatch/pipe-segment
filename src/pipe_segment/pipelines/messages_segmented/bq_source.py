import logging

from pipe_segment.utils.bq_tools import BigQueryHelper


logger = logging.getLogger(__name__)


class BigQuerySource:
    def __init__(
        self,
        bq_helper: BigQueryHelper,
        table_id: str,
    ):
        logger.info(f"Building BigQuerySource for source {table_id}")
        self._bq_helper = bq_helper
        self.table_id = table_id
        self._check_sharded_or_partitioned(table_id)

    def _check_sharded_or_partitioned(self, table_id):
        table = self._bq_helper.fetch_table(table_id)
        if table is not None:
            self._initialize_partitioned_table(table)
            return

        table = self._bq_helper.fetch_table(f"{table_id}*")
        if table is not None:
            self._initialize_sharded_table()
            return

        raise ValueError("Source table is neither sharded or partitioned")

    def _initialize_partitioned_table(self, table):
        logger.info(f"Table {self.table_id} is a partitioned table")
        self.date_format = "%Y-%m-%d"
        self._table_suffix = ''

        if table.time_partitioning:
            self.filtering_field = f"DATE({table.time_partitioning.field})"
            logger.info(f'Table {self.table_id} is partitioned on field {self.filtering_field}')
        else:
            self.filtering_field = "DATE(timestamp)"
            logger.info(
                f'Table {self.table_id} is not time partitioned, defaulting to timestamp filtering'
            )

    def _initialize_sharded_table(self):
        logger.info(f"Table {self.table_id} is a date sharded table")
        self._table_suffix = '*'
        self.date_format = "%Y%m%d"
        self.filtering_field = "_TABLE_SUFFIX"

    @property
    def qualified_source(self):
        return f"{self.table_id}{self._table_suffix}"
