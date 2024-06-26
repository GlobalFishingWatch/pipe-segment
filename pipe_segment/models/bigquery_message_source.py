import logging

from pipe_segment.utils.bqtools import BigQueryTools


class BigQueryMessagesSource:
    def __init__(
        self,
        bqtools: BigQueryTools,
        table_id: str,
    ):
        self._bqtools = bqtools
        self._table_id = table_id
        self._check_sharded_or_partitioned(table_id)

    def _check_sharded_or_partitioned(self, table_id):
        # TODO: clean a little bit this code.

        # Default values (partitioned)
        self.filtering_field = "timestamp"
        self.date_format = "%Y-%m-%d"
        self.table_suffix = ''

        try:
            full_table_id = table_id.replace("bq://", "").replace(":", ".")
            _, dataset_id, table_name = full_table_id.split(".")
            self._table_suffix = ''
            # When the table is found as provided in the arguments is partitioned
            self._table = self._bqtools.get_table(dataset_id, table_name)

            if self._table.time_partitioning:
                self.filtering_field = self._table.time_partitioning.field
                logging.info(f'Table {table_id} is partitioned on field {self.filtering_field}')

        except Exception as e:
            if "Not found" in str(e):
                logging.info(
                    f'Table {table_id} does not exists, checking if it\'s a sharded table')
                # Ensure the table is sharded
                self._table_suffix = '*'
                try:
                    self._bqtools.get_table(dataset_id, f"{table_name}{self._table_suffix}")
                    logging.info(f'Table {table_id}* is date sharded')

                    self.filtering_field = "_TABLE_SUFFIX"
                    self.date_format = "%Y%m%d"
                except Exception as ex:
                    if "Not found" in str(ex):
                        logging.error(f'Table sharded {table_id}* could not be found.')
                    raise ex
            else:
                raise e

    @property
    def qualified_source_messages(self) -> str:
        return f"{self._table_id}{self._table_suffix}"
