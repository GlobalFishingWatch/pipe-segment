import apache_beam as beam
from apache_beam import PTransform

from pipe_tools.coders import ReadAsJSONDict
from pipe_tools.timestamp import ParseBeamBQStrTimestampDoFn


class BigQuerySource(PTransform):
    def __init__(self, query=None, table=None):
        self.query = query
        self.table = table


    def expand(self, pcoll):
        source = beam.io.gcp.bigquery.BigQuerySource(query=self.query, table=self.table)
        pcoll = (pcoll
                 | "ReadFromBigQuery" >> ReadAsJSONDict(source)
                 | "ConvertTimestamp" >> beam.ParDo(ParseBeamBQStrTimestampDoFn())
                )

        return pcoll