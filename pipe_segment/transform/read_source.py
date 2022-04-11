import apache_beam as beam
import apache_beam.io.gcp.internal.clients.bigquery as beambq
import logging

from apache_beam.io.gcp.bigquery_tools import BigQueryWrapper

from datetime import datetime as dt, timezone as tz


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
    def __init__(self, source_table, start_ts, end_ts, validate=True):
        self.source_table = source_table.replace('bq://','').replace(':', '.')
        logging.debug(f'ReadSource source table {self.source_table}')
        self.project, self.dataset, self.table_id = self.source_table.split('.')
        self.start_ts = int(start_ts)
        self.end_ts = int(end_ts)
        self.validate = validate

        # Check if table exists (idea of pipeline construction vs execution)
        # The wrapper let me raise the httperror
        client = BigQueryWrapper()
        start_dt = dt.fromtimestamp(start_ts, tz.utc)
        self.table_info = client.get_table(self.project, self.dataset, f'{self.table_id}{start_dt.strftime("%Y%m%d")}')

    @property
    def schema(self):
        """
        Returns the Beam TableSchema of a date sharded BQ table that is reference in source
        """
        # client = bigquery.Client(self.project)
        # query_job = client.query(f"""
        #     SELECT table_name FROM `{self.dataset}.INFORMATION_SCHEMA.TABLES`
        #     WHERE table_name LIKE '{self.table_id}%' ORDER BY table_name ASC LIMIT 1
        # """)
        # table_ids = [x[0] for x in query_job]
        # table = client.get_table('.'.join([self.dataset, table_ids[0]]))

        # def build_beam_field(name, field_type, mode):
        #     tfs = beambq.TableFieldSchema()
        #     tfs.name = name
        #     tfs.type = field_type
        #     tfs.mode = mode
        #     return tfs
        # return beambq.TableSchema(fields=[build_beam_field(x.name, x.field_type, x.mode) for x in self.table_info.schema])
        return self.table_info.schema


    def expand(self, pcoll):
        return (
            pcoll
            | self.read_source()
        )

    def read_source(self):
        query = SOURCE_QUERY_TEMPLATE.format(
            source_table=self.source_table,
            start_ts=self.start_ts,
            end_ts=self.end_ts,
        )
        return beam.io.ReadFromBigQuery(
            query = query,
            validate=self.validate,
            use_standard_sql=True,
        )
