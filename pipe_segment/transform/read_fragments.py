import logging
from datetime import datetime, timedelta

import apache_beam as beam
from google.api_core.exceptions import BadRequest
from google.cloud import bigquery


logger = logging.getLogger(__name__)


class ReadFragments(beam.PTransform):
    def __init__(
        self, source, end_date, ssvid_filter_query
    ):
        self.source = source
        self.end_date = end_date
        self.ssvid_filter_query = ssvid_filter_query

    def ssvid_filter_query_condition(self):
        if self.ssvid_filter_query is not None:
            return f" AND ssvid IN ({self.ssvid_filter_query})"
        else:
            return ""

    def render_query(self):
        query = f"""
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
        WHERE
            _TABLE_SUFFIX <= "{self.end_date:%Y%m%d}"
            {self.ssvid_filter_query_condition()}
        ORDER BY ssvid, timestamp
        """

        logger.info(f"Fragments query:\n{query}")
        return query

    def expand(self, pcoll):
        return (
            pcoll
            | "ReadFragments" >> beam.io.ReadFromBigQuery(
                query=self.render_query(),
                use_standard_sql=True,
            )
        )
