import logging
from datetime import datetime

import apache_beam as beam


logger = logging.getLogger(__name__)


class ReadFragments(beam.PTransform):
    """Reads the fragments since the condition"""
    def __init__(
        self,
        source: str,
        start_date: datetime.date,
        end_date: datetime.date,
        labels: dict,
    ):
        self.labels = labels
        self.source = source
        self.start_date = start_date
        self.end_date = end_date

    def make_query(self) -> str:
        condition = ""
        if self.start_date is not None:
            condition = (f"WHERE date(timestamp) >= {self.start_date} "
                         f"AND date(timestamp) <= {self.end_date}")  # includes the end_date.
        query = f"""
            SELECT
              CAST(UNIX_MICROS(timestamp) AS FLOAT64) / 1000000  AS timestamp,
              CAST(UNIX_MICROS(first_msg_timestamp) AS FLOAT64) / 1000000 AS first_msg_timestamp,
              CAST(UNIX_MICROS(last_msg_timestamp) AS FLOAT64) / 1000000 AS last_msg_timestamp,
              * except (
                  timestamp,
                  first_msg_timestamp,
                  last_msg_timestamp
              )
            FROM `{self.source}`
            {condition}
        """
        logger.debug(f"Emitting read fragments query:\n{query}")
        return query

    def expand(self, pcoll):
        return (pcoll | "ReadFragments"
                >> beam.io.ReadFromBigQuery(
                    query=self.make_query(),
                    method=beam.io.ReadFromBigQuery.Method.EXPORT,
                    bigquery_job_labels=self.labels,
                    use_standard_sql=True
                ))
