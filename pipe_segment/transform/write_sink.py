import apache_beam as beam
from ..tools import datetime_from_timestamp


class WriteSink(beam.PTransform):
    def __init__(self, sink_table, schema, key="timestamp"):
        self.sink_table = sink_table.replace('bq://', '')
        self.schema = schema
        self.key = key

    def expand(self, pcoll):
        return (
            pcoll
            | self.write_sink()
        )

    def write_sink(self):
        def compute_table(message):
            table_suffix = datetime_from_timestamp(message[self.key]).strftime("%Y%m%d")
            return f"{self.sink_table}{table_suffix}"

        return beam.io.WriteToBigQuery(
            compute_table,
            schema={"fields": self.schema},
            write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
            create_disposition=beam.io.BigQueryDisposition.CREATE_NEVER,
        )
