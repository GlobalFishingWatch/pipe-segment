import logging

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

    def table_present(self):
        client = bigquery.Client(self.project)
        query = f'''SELECT COUNT(*) cnt FROM `{self.source}*`
                     WHERE _TABLE_SUFFIX BETWEEN "{self.start_date:%Y%m%d}"
                     AND "{self.end_date:%Y%m%d}"'''
        logging.info(f"QUERY:\n{query}")
        request = client.query(query)
        try:
            [row] = request.result()
            if row.cnt == 0:
                return False
        except BadRequest as err:
            return False
            logging.info(
                f"Could not query existing table. Ignore if this is first run: {err}"
            )
        else:
            return True

    @property
    def query(self):
        query = f"""
        SELECT *
        FROM (
            SELECT
              CAST(UNIX_MILLIS(timestamp) AS FLOAT64) / 1000  AS timestamp,
              CAST(UNIX_MILLIS(first_msg_of_day_timestamp) AS FLOAT64) / 1000
                    AS first_msg_of_day_timestamp,
              CAST(UNIX_MILLIS(last_msg_of_day_timestamp) AS FLOAT64) / 1000
                    AS last_msg_of_day_timestamp,
              CAST(UNIX_MILLIS(first_timestamp) AS FLOAT64) / 1000
                    AS first_timestamp,
                * except (
                        timestamp, 
                        first_msg_of_day_timestamp, 
                        last_msg_of_day_timestamp,
                        first_timestamp
                    )
            FROM `{self.source}*`
            WHERE _TABLE_SUFFIX BETWEEN 
             '{self.start_date:%Y%m%d}' AND '{self.end_date:%Y%m%d}'
            ORDER BY ssvid, timestamp
        )
        """
        return query

    def expand(self, pcoll):
        if self.create_if_missing and not self.table_present():
            return pcoll | beam.Create([])
        else:
            return pcoll | beam.io.ReadFromBigQuery(
                query=self.query, use_standard_sql=True
            )
