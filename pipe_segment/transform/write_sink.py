import apache_beam as beam
from ..tools import datetimeFromTimestamp


class WriteSink(beam.PTransform):
    """Writes the partitioned tables specifing each property."""
    def __init__(
        self,
        sink_table: str,
        schema: dict,
        description: str = None,
        key: str = "timestamp",
        clustering_fields: list = ["ssvid"]
    ):
        self.sink_table = sink_table.replace("bq://", "")
        self.schema = schema
        self.description = description
        self.key = key
        self.clustering_fields = clustering_fields.insert(0, key)

    def expand(self, pcoll):
        return (
            pcoll
            | self.write_sink()
        )

    def write_sink(self):
        def compute_table_name(elem):
            if self.sink_table.endswith('fragments'):
                print(elem)
            return f'world-fishing-827:{self.sink_table}'

        return beam.io.WriteToBigQuery(
            table=compute_table_name,
            schema=self.schema,
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
            additional_bq_parameters={
                "timePartitioning": {
                    "type": "MONTH",
                    "field": self.key,
                    "requirePartitionFilter": False
                }, "clustering": {
                    "fields": self.clustering_fields
                }, "destinationTableProperties": {
                    "description": self.description
                }
            }
        )
