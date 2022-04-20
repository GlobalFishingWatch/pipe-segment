import apache_beam as beam

BQ_PARAMS = {
    "destinationTableProperties": {
        "description": "Daily satellite messages.",
    },
}

class WriteSink(beam.PTransform):
    def __init__(self, sink_table, schema, description=None):
        self.sink_table = sink_table.replace('bq://','')
        self.schema = schema
        self.description = description

    def expand(self, pcoll):
        return (
            pcoll
            | self.write_sink()
        )

    def write_sink(self):
        bq_params_cp = dict(BQ_PARAMS)
        bq_params_cp['destinationTableProperties']['description'] = self.description

        def compute_table(message):
            table_suffix = message["timestamp"].strftime("%Y%m%d")
            return "{}{}".format(self.sink_table, table_suffix)

        return beam.io.WriteToBigQuery(
            compute_table,
            schema=self.schema,
            additional_bq_parameters=bq_params_cp,
            write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
        )

