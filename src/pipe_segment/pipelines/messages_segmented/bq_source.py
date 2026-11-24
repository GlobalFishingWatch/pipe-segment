"""The source tables of the messages."""

import logging

from gfw.common.bigquery.helper import BigQueryHelper


logger = logging.getLogger(__name__)


class BigQuerySource:
    """A partitioned table with messages, and the field to filter its days with.

    Args:
        bq_helper:
            Helper to look up the table.

        table_id:
            The table. It fails if it does not exist.
    """

    def __init__(self, bq_helper: BigQueryHelper, table_id: str) -> None:
        """Looks up the table to find out its partition field."""
        logger.info(f"Building BigQuerySource for source {table_id}")
        self.table_id = table_id
        table = bq_helper.client.get_table(table_id)

        if table.time_partitioning and table.time_partitioning.field:
            field = table.time_partitioning.field
            logger.info(f"Table {table_id} is partitioned on field {field}")
        else:
            field = "timestamp"
            logger.info(f"Table {table_id} is not partitioned by a field, filtering by timestamp")

        self.filtering_field = f"DATE({field})"
