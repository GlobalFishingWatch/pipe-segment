import logging

import apache_beam as beam

from pipe_tools.coders import JSONDictCoder
from pipe_tools.timestamp import ParseBeamBQStrTimestampDoFn
from pipe_tools.io.bigquery import QueryHelper
from pipe_segment.io.gcp import parse_gcp_path


class GCPSource(beam.PTransform):
    def __init__(self, gcp_path, first_date_ts=None, last_date_ts=None):
        self.service, self.path = parse_gcp_path(gcp_path)
        self.first_date_ts = first_date_ts
        self.last_date_ts = last_date_ts
        self.schema = None
        self.query = None

        if self.service == 'table':
            helper = QueryHelper(table=self.path,
                                  first_date_ts=self.first_date_ts,
                                  last_date_ts=self.last_date_ts,
                                  use_legacy_sql=False)
            self.query = helper.build_query()
            if self.schema is None:
                self.schema = helper.table_schema
        elif self.service == 'query':
            self.query = self.path

    def expand_file(self, pcoll):
        source = beam.io.ReadFromText(file_pattern=self.path, coder=JSONDictCoder())
        return pcoll | "ReadFromFile" >> source


    def expand_query(self, pcoll):
        if self.schema:
            ts_fields = {f.name for f in self.schema.fields if f.type=='TIMESTAMP'}
        else:
            ts_fields = {'timestamp'}

        source = beam.io.gcp.bigquery.BigQuerySource(query=self.query, use_standard_sql=True)
        return (
            pcoll
            | "ReadFromBigQuery" >> beam.io.Read(source)
            | "ConvertTimestamp" >> beam.ParDo(ParseBeamBQStrTimestampDoFn(fields=list(ts_fields)))
        )

    def expand(self, pcoll):
        if self.service == 'file':
            return self.expand_file(pcoll)
        elif self.service in ('table', 'query'):
            return self.expand_query(pcoll)

        raise RuntimeError("Unknown GCP service type: %s" % self.service)

