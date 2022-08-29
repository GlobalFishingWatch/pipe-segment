import apache_beam as beam
import logging

SOURCE_QUERY_TEMPLATE = """
    SELECT
      *
    FROM
      `{source_table}*`
    WHERE
      _TABLE_SUFFIX BETWEEN FORMAT_TIMESTAMP('%Y%m%d', TIMESTAMP_SECONDS({start_ts}))
      AND FORMAT_TIMESTAMP('%Y%m%d', TIMESTAMP_SECONDS({end_ts}))
      AND TRUE
"""

class ReadSource(beam.PTransform):
    def __init__(self, source_table, start_ts, end_ts):
        self.source_table = source_table.replace("bq://", "").replace(":", ".")
        self.start_ts = int(start_ts)
        self.end_ts = int(end_ts)

    def read_source(self):
        query = SOURCE_QUERY_TEMPLATE.format(
            source_table=self.source_table,
            start_ts=self.start_ts,
            end_ts=self.end_ts,
        )
        return beam.io.ReadFromBigQuery(
            query=query,
            use_standard_sql=True,
        )

    def expand(self, pcoll):
        return pcoll | self.read_source()
