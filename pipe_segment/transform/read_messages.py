import logging
from typing import List

import apache_beam as beam
from pipe_segment.models.bigquery_message_source import BigQueryMessagesSource


logger = logging.getLogger(__name__)


class ReadMessages(beam.PTransform):
    def __init__(
        self,
        sources,
        start_date,
        end_date,
        ssvid_filter_query,
    ):
        self.sources: List[BigQueryMessagesSource] = sources
        self.start_date = start_date
        self.end_date = end_date
        self.ssvid_filter_query = ssvid_filter_query

    def query(self, source: BigQueryMessagesSource):
        query = f"""
        SELECT *
        FROM (
            SELECT
              CAST(UNIX_MICROS(timestamp) AS FLOAT64) / 1000000  AS timestamp,
                * except (timestamp)
            FROM `{source.qualified_source_messages}`
            WHERE {source.filter_messages(start_date=self.start_date, end_date=self.end_date)}
        )
        """
        if self.ssvid_filter_query is not None:
            query = f"{query} WHERE ssvid IN ({self.ssvid_filter_query})"

        logger.debug(f"QUERY:\n{query}")

        return query

    def expand(self, pcoll):
        return (
            pcoll
            | f"ReadFromSrc{i}"
            >> beam.io.ReadFromBigQuery(query=self.query(src), use_standard_sql=True)
            for i, src in enumerate(self.sources)
        ) | beam.Flatten()
