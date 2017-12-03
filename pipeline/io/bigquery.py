from datetime import datetime

from apache_beam.io.gcp.internal.clients.bigquery import TableSchema

from pipe_tools.io.bigquery import BigQueryWrapper
from pipe_tools.io.bigquery import decode_table_ref
from pipe_tools.io.bigquery import encode_table_ref
from pipe_tools.timestamp import timestampFromDatetime
from pipe_tools.timestamp import datetimeFromTimestamp


class QueryBuilder():
    """
    Helper class that builds date range queries and fetches table schemas
    """

    def __init__(self, table, dataset=None, project=None, first_date_ts=None, last_date_ts=None):
        self.first_date_ts = first_date_ts
        self.last_date_ts = last_date_ts

        self.table_ref = decode_table_ref(table, dataset, project)
        table_id = self.table_ref.tableId

        if first_date_ts is not None:
            assert last_date_ts is not None, 'Must supply both first_date and last_date, or neither'
            dt = datetimeFromTimestamp(first_date_ts)
            table_id = '{}{}'.format(table_id, dt.strftime('%Y%m%d'))

        client = BigQueryWrapper()
        self._table_info = client._get_table(self.table_ref.projectId, self.table_ref.datasetId, table_id)

    @staticmethod
    def _date_to_sql_timestamp(date):
        if isinstance(date, basestring):
            return 'TIMESTAMP({})'.format(date)
        elif isinstance(date, datetime):
            timestamp = timestampFromDatetime(date)
        else:
            # assume that date is already a timestamp
            timestamp = date
        return 'SEC_TO_TIMESTAMP({})'.format(int(timestamp))

    def build_query(self, include_fields=None, where_sql=None):
        fields = ','.join(include_fields or '*')
        table = '[{}]'.format(encode_table_ref(self.table_ref))
        where = where_sql or 'True'
        if self.is_date_sharded:
            table_params=dict(table=table,
                        first_date=self._date_to_sql_timestamp(self.first_date_ts),
                        last_date=self._date_to_sql_timestamp(self.last_date_ts))
            table = 'TABLE_DATE_RANGE({table}, {first_date}, {last_date})'.format(**table_params)

        return "SELECT {fields} FROM {table} WHERE {where}".format(fields=fields,table=table,where=where)

    def filter_table_schema(self, include_fields=None):
        if include_fields is None:
            schema = self.table_schema
        else:
            schema = TableSchema()
            schema.fields = [field for field in self.table_schema.fields if field.name in include_fields]
        return schema

    @property
    def table_info(self):
        return self._table_info

    @property
    def table_schema(self):
        return self._table_info.schema

    @property
    def is_date_sharded(self):
        return self.first_date_ts is not None