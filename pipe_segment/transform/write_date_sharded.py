import apache_beam as beam
from ..timestamp import datetimeFromTimestamp


class WriteDateSharded(beam.PTransform):
    def __init__(self, sink, project, schema, key="timestamp"):
        self.sink = sink
        self.project = project
        self.schema = schema
        self.key = key

    def compute_table_for_event(self, msg):
        dt = datetimeFromTimestamp(msg["timestamp"]).date()
        return f"{self.project}:{self.sink}{dt:%Y%m%d}"

    def expand(self, pcoll):
        return pcoll | beam.io.WriteToBigQuery(
            self.compute_table_for_event,
            schema=self.schema,
            write_disposition="WRITE_TRUNCATE",
        )
