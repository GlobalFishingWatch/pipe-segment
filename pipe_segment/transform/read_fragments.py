import logging
from datetime import datetime, timedelta

import apache_beam as beam
from google.api_core.exceptions import BadRequest
from google.cloud import bigquery


class ReadFragments(beam.PTransform):
    def __init__(
        self, source, start_date, end_date, create_if_missing=False, project=None
    ):
        self.source = source
        self.project = project
        self.start_date = start_date
        self.end_date = end_date
        self.create_if_missing = create_if_missing

    def first_table_date(self):
        client = bigquery.Client(self.project)
        condition = self.query_condition(self.start_date, self.end_date)
        query = f"""SELECT MIN(_TABLE_SUFFIX) min_suffix FROM `{self.source}*`
                     WHERE {condition}"""
        logging.info(f"QUERY:\n{query}")
        request = client.query(query)
        try:
            [row] = request.result()
        except BadRequest as err:
            logging.info(
                f"Could not query existing table. Ignore if this is first run: {err}"
            )
            return None
        else:
            return datetime.strptime(row.min_suffix, "%Y%m%d").date() if row.min_suffix != None else None

    def query_condition(self, start_date, end_date):
        if start_date is None:
            return f'''_TABLE_SUFFIX <= "{end_date:%Y%m%d}"'''
        else:
            return f'''_TABLE_SUFFIX BETWEEN "{start_date:%Y%m%d}"
                         AND "{end_date:%Y%m%d}"'''

    def make_queries(self, first_table_date):
        if self.start_date is None:
            start_date = first_table_date
        else:
            start_date = max(first_table_date, self.start_date)
        while start_date <= self.end_date:
            next_start_date = start_date + timedelta(days=365)
            end_date = min(next_start_date - timedelta(days=1), self.end_date)
            condition = self.query_condition(start_date, end_date)
            query = f"""
            SELECT *
            FROM (
                SELECT
                  CAST(UNIX_MICROS(timestamp) AS FLOAT64) / 1000000  AS timestamp,
                  CAST(UNIX_MICROS(first_msg_timestamp) AS FLOAT64) / 1000000
                        AS first_msg_timestamp,
                  CAST(UNIX_MICROS(last_msg_timestamp) AS FLOAT64) / 1000000
                        AS last_msg_timestamp,
                    * except (
                            timestamp, 
                            first_msg_timestamp, 
                            last_msg_timestamp
                        )
                FROM `{self.source}*`
                WHERE {condition}
                ORDER BY ssvid, timestamp
            )
            """
            logging.info(f"Emitting read fragments query:\n{query}")
            yield query
            start_date = next_start_date

    def expand(self, pcoll):
        first_table_date = self.first_table_date()
        if self.create_if_missing and first_table_date is None:
            return pcoll | beam.Create([])
        else:
            queries = [
                pcoll
                | f"ReadFragments{i}"
                >> beam.io.ReadFromBigQuery(query=query, use_standard_sql=True)
                for i, query in enumerate(self.make_queries(first_table_date))
            ]
            return queries | beam.Flatten()
