from apache_beam import DoFn
from apache_beam import ParDo
from apache_beam import PTransform
from apache_beam import io

from pipeline.coders import str2timestamp


class ParseTimestampsDoFn(DoFn):
    """Convert timestamp fields from string representation to Timestamp"""
    def __init__(self, timestamp_fields):
        self.timestamp_fields = timestamp_fields

    def process(self, msg):
        msg = dict(msg)
        for field in self.timestamp_fields:
            msg[field] = str2timestamp(msg['timestamp'])
        yield msg

class BigQuerySource(PTransform):
    def __init__(self, query=None, table=None,  schema=None):
        self.query = query
        self.table = table
        self.schema = schema

    def _timestamp_fields_from_schema(self, schema):
        if schema is None:
            return []
        else:
            return [field.name for field in schema.fields if field.type == 'TIMESTAMP']

    def expand(self, pcoll):
        pcoll = pcoll | io.Read(io.gcp.bigquery.BigQuerySource(query=self.query, table=self.table))

        timestamp_fields = self._timestamp_fields_from_schema(self.schema)
        if timestamp_fields:
            pcoll = pcoll | "ParseTimestamps" >> ParDo(ParseTimestampsDoFn(timestamp_fields))
        return pcoll

