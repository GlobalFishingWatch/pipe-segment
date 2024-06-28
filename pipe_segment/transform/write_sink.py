import apache_beam as beam
from ..tools import datetime_from_timestamp


class WriteSink(beam.PTransform):
    def __init__(self, sink_table, schema, description=None, key="timestamp"):
        self.sink_table = sink_table.replace('bq://', '')
        self.schema = schema
        self.description = description
        self.key = key

    def expand(self, pcoll):
        return (
            pcoll
            | self.write_sink()
        )

    def write_sink(self):
        bq_params_cp = {"destinationTableProperties": {"description": self.description}}

        def compute_table(message):
            table_suffix = datetime_from_timestamp(message[self.key]).strftime("%Y%m%d")
            return f"{self.sink_table}{table_suffix}"

        return beam.io.WriteToBigQuery(
            compute_table,
            schema=self.schema,
            additional_bq_parameters=bq_params_cp,
            write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
        )
