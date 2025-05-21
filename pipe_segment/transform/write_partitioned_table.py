import apache_beam as beam


class WritePartitionedTable(beam.PTransform):
    """Writes the partitioned tables specifing each property."""
    def __init__(
        self,
        table: str,
        schema: dict,
        description: str = None,
        partition_field: str = "timestamp"
    ):
        self.table = table.replace('bq://', '')
        self.schema = schema
        self.description = description
        self.partition_field = partition_field

    def expand(self, pcoll):
        return (
            pcoll
            | self.write_partitioned_table()
        )

    def write_partitioned_table(self):
        return beam.io.WriteToBigQuery(
            table=self.table,
            schema=self.schema,
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
            additional_bq_parameters={
                "timePartitioning": {
                    "type": "MONTH",
                    "field": self.partition_field,
                    "requirePartitionFilter": False
                }, "clustering": {
                    "fields": [self.partition_field]
                }
            }
        )
