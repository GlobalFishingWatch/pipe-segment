import apache_beam as beam

from apache_beam import DoFn
from apache_beam import ParDo
from apache_beam import PTransform
from apache_beam import io
from apache_beam.io.gcp.internal.clients.bigquery import TableSchema
from pipe_tools.io.bigquery import BigQueryWrapper
from pipe_tools.io.bigquery import decode_table_ref
from pipe_tools.timestamp import ParseBeamBQStrTimestampDoFn
from pipe_tools.coders import ReadAsJSONDict


# from pipeline.coders import str2timestamp


# class ParseTimestampsDoFn(DoFn):
#     """Convert timestamp fields from string representation to Timestamp"""
#     def __init__(self, timestamp_fields):
#         self.timestamp_fields = timestamp_fields
#
#     def process(self, msg):
#         msg = dict(msg)
#         for field in self.timestamp_fields:
#             msg[field] = str2timestamp(msg['timestamp'])
#         yield msg

# class BigQuerySource(PTransform):
#     def __init__(self, query=None, table=None,  schema=None):
#         self.query = query
#         self.table = table
#         self.schema = schema
#
#     def _timestamp_fields_from_schema(self, schema):
#         if schema is None:
#             return []
#         else:
#             return [field.name for field in schema.fields if field.type == 'TIMESTAMP']
#
#     def expand(self, pcoll):
#         pcoll = pcoll | io.Read(io.gcp.bigquery.BigQuerySource(query=self.query, table=self.table))
#
#         timestamp_fields = self._timestamp_fields_from_schema(self.schema)
#         if timestamp_fields:
#             pcoll = pcoll | "ParseTimestamps" >> ParDo(ParseTimestampsDoFn(timestamp_fields))
#         return pcoll


# class DateRange():
#     def __init__(self, start_date, end_date):
#         pass

#
# class BigQueryTableDateRange():
#     def __init__(self, table, dataset=None, project=None):
#         client = BigQueryWrapper()
#         table_ref = decode_table_ref(table, dataset, project)
#         self._schema = client.get_table_schema()
#         self._is_date_partitioned = client.get_table_is_date_partitioned()
#
#     def query(self, include_fields=None, first_date=None, last_date=None, sql_where=None):
#         raise NotImplementedError
#
#     def schema(self, include_fields=None):
#         if include_fields is None:
#             return self._schema
#         else:
#             s = TableSchema()
#             s.fields = [field for field in self._schema.fields if field.name in include_fields]
#             return s
#
#     def is_date_partitioned(self):
#         raise NotImplementedError


class BigQuerySource(PTransform):
    def __init__(self, query=None, table=None):
        self.query = query
        self.table = table


    def expand(self, pcoll):
        source = io.gcp.bigquery.BigQuerySource(query=self.query, table=self.table)
        pcoll = (pcoll
                 | "ReadFromBigQuery" >> ReadAsJSONDict(source)
                 | "ConvertTimestamp" >> beam.ParDo(ParseBeamBQStrTimestampDoFn())
                )

        return pcoll