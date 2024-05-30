from datetime import datetime
import logging
from jinja2 import Template

from pipe_segment.utils.bq import get_bq_table


class BigQueryMessagesSource:

    def __init__(
        self,
        table_id: str,
        filter_template: str = None
    ):
        self.table_id = table_id
        self.filter_template = filter_template

        # Checks whether the provided table is date sharded or partitioned
        try:
            project_id, dataset_id, table_name = table_id.replace("bq://", "") \
                .replace(":", ".") \
                .split(".")
            self._table_suffix = ''
            # When the table is found as provided in the arguments is partitioned
            self._table = get_bq_table(project_id, dataset_id, table_name)

            if self._table.time_partitioning:
                partitioning_field = self._table.time_partitioning.field
                logging.info(f'Table {table_id} is partitioned on field {partitioning_field}')
                self._filtering_field = partitioning_field
            else:
                # Use timestamp field by default
                self._filtering_field = "timestamp"

            self._date_format = "%Y-%m-%d"
        except Exception as e:
            if "Not found" in str(e):
                logging.info(
                    f'Table {table_id} does not exists, checking if it\'s a sharded table')
                # Ensure the table is sharded
                self._table_suffix = '*'
                try:
                    self._table = get_bq_table(project_id, dataset_id,
                                               f"{table_name}{self._table_suffix}")
                    self._filtering_field = "_TABLE_SUFFIX"
                    self._date_format = "%Y%m%d"
                    logging.info(f'Table {table_id}* is date sharded')
                except Exception as ex:
                    if "Not found" in str(ex):
                        logging.error(f'Table sharded {table_id}* could not be found.')
                    raise ex
            else:
                raise e

    def filter_messages(self, start_date: datetime, end_date: datetime) -> str:
        template = self.filter_template
        if template is None:
            template = "{{ filter_field }} " + \
                "BETWEEN '{{ start_date.strftime(date_format) }}' " + \
                " AND '{{ end_date.strftime(date_format) }}'"

        filter = Template(template).render({'start_date': start_date,
                                            'end_date': end_date,
                                            'filter_field': self._filtering_field,
                                            'date_format': self._date_format,
                                            })
        return filter

    @property
    def qualified_source_messages(self) -> str:
        return f"{self.table_id}{self._table_suffix}"
