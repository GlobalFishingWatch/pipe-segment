import logging

from jinja2 import Template

import apache_beam as beam
from pipe_segment.models.bigquery_message_source import BigQueryMessagesSource


logger = logging.getLogger(__name__)

FILTER_TEMPLATE = """
    DATE({{ filter_field }})
    BETWEEN '{{ start_date.strftime(date_format) }}'
    AND '{{ end_date.strftime(date_format) }}'
"""


class ReadMessages(beam.PTransform):
    def __init__(
        self,
        bqtools,
        sources,
        start_date,
        end_date,
        ssvid_filter_query=None,
    ):
        self.bqtools = bqtools
        self.sources = sources
        self.start_date = start_date
        self.end_date = end_date
        self.ssvid_filter_query = ssvid_filter_query

    def query(self, source: str):
        bq_source = BigQueryMessagesSource(self.bqtools, source)

        query = f"""
        SELECT *
        FROM (
            SELECT
              CAST(UNIX_MICROS(timestamp) AS FLOAT64) / 1000000  AS timestamp,
                * except (timestamp)
            FROM `{bq_source.qualified_source_messages}`
            WHERE {self._build_filter(bq_source)}
        )
        """
        if self.ssvid_filter_query is not None:
            query = f"{query} WHERE ssvid IN ({self.ssvid_filter_query})"

        logger.debug(f"QUERY:\n{query}")

        return query

    def _build_filter(self, source: BigQueryMessagesSource):
        return Template(FILTER_TEMPLATE).render({
            'start_date': self.start_date,
            'end_date': self.end_date,
            'filter_field': source.filtering_field,
            'date_format': source.date_format,
        })

    def expand(self, pcoll):
        return [
            pcoll
            | f"ReadFromSrc{i}"
            >> beam.io.ReadFromBigQuery(query=self.query(src), use_standard_sql=True)
            for i, src in enumerate(self.sources)
        ] | beam.Flatten()
