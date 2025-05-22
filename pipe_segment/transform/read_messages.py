import logging

from jinja2 import Template

import apache_beam as beam
from pipe_segment.utils.bq_source import BigQuerySource


logger = logging.getLogger(__name__)


FILTER_TEMPLATE = """
    {{ filter_field }}
    BETWEEN '{{ start_date.strftime(date_format) }}'
    AND '{{ end_date.strftime(date_format) }}'
"""


class ReadMessages(beam.PTransform):
    def __init__(
        self,
        bq_helper,
        sources,
        start_date,
        end_date,
        ssvid_filter_query=None,
    ):
        self.bq_helper = bq_helper
        self.sources = sources
        self.start_date = start_date
        self.end_date = end_date
        self.ssvid_filter_query = ssvid_filter_query

    def build_query(self, source: str):
        bq_source = BigQuerySource(self.bq_helper, source)

        query = f"""
        SELECT *
        FROM (
            SELECT
              CAST(UNIX_MICROS(timestamp) AS FLOAT64) / 1000000  AS timestamp,
                * except (timestamp)
            FROM `{bq_source.qualified_source}`
            WHERE {self._build_filter(bq_source)}
        )
        """
        if self.ssvid_filter_query is not None:
            query = f"{query} WHERE ssvid IN ({self.ssvid_filter_query})"

        logger.debug(f"QUERY:\n{query}")

        return query

    def _build_filter(self, bq_source: BigQuerySource):
        return Template(FILTER_TEMPLATE).render({
            'start_date': self.start_date,
            'end_date': self.end_date,
            'filter_field': bq_source.filtering_field,
            'date_format': bq_source.date_format,
        })

    def expand(self, pcoll):
        return [
            pcoll
            | f"ReadFromSrc{i}"
            >> beam.io.ReadFromBigQuery(query=self.build_query(src), use_standard_sql=True)
            for i, src in enumerate(self.sources)
        ] | beam.Flatten()
