import logging
import typing
from datetime import datetime

import apache_beam as beam
from pipe_segment.utils.bq_tools import BigQueryTools
from google.cloud.exceptions import NotFound


logger = logging.getLogger(__name__)


class ReadFragments(beam.PTransform):
    """Reads the fragments since the condition"""
    def __init__(
        self,
        source: str,
        start_date: datetime.date,
        end_date: datetime.date,
        project: str = None,
        labels: typing.List = []
    ):
        self.source = source
        self.bqtools = BigQueryTools.build(project=project)
        self.start_date = start_date
        self.end_date = end_date
        self.labels = {x.split('=')[0]: x.split('=')[1] for x in labels}

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

    def is_table_missing(self) -> bool:
        """Returns True if the source table is missing."""
        try:
            d, t = self.source.split('.')
            self.bqtools.get_bq_table(d, t)
            return False
        except NotFound:
            return True

    def expand(self, pcoll):
        if self.is_table_missing():
            return pcoll | beam.Create([])
        return (pcoll | "ReadFragments"
                >> beam.io.ReadFromBigQuery(
                    query=self.make_query(),
                    method=beam.io.ReadFromBigQuery.Method.DIRECT_READ,
                    bigquery_job_labels=self.labels,
                    use_standard_sql=True
                ))
