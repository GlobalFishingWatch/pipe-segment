import apache_beam as beam

from apache_beam.io.gcp.bigquery import BigQueryDisposition

from pipe_tools.coders import JSONDictCoder
from pipe_tools.timestamp import ParseBeamBQStrTimestampDoFn
from pipe_tools.io.bigquery import QueryHelper
from pipe_tools.io import WriteToBigQueryDateSharded

from pipe_segment.io.gcp import parse_gcp_path


class GCPSink(beam.PTransform):
    def __init__(self, gcp_path, schema=None, temp_gcs_location=None, temp_shards_per_day=None):
        self.service, self.path = parse_gcp_path(gcp_path)
        self.schema = schema
        self.temp_gcs_location = temp_gcs_location
        self.temp_shards_per_day = temp_shards_per_day

    def expand_file(self, pcoll):
        sink = beam.io.WriteToText(file_path_prefix=self.path, coder=JSONDictCoder())
        return pcoll | "WriteToFile" >> sink

    def expand_table(self, pcoll):
        sink = WriteToBigQueryDateSharded(
            temp_gcs_location=self.temp_gcs_location,
            table=self.path,
            schema=self.schema,
            write_disposition=BigQueryDisposition.WRITE_TRUNCATE,
            temp_shards_per_day=self.temp_shards_per_day
        )
        return pcoll | "WriteToBigquery" >> sink

    def expand(self, pcoll):
        if self.service == 'file':
            return self.expand_file(pcoll)
        elif self.service == 'table':
            return self.expand_table(pcoll)
        elif self.service == 'query':
            raise RuntimeError("Cannot use a query as a sink")
        raise RuntimeError("Unknown GCP service type: %s" % self.service)

