import logging

from datetime import date

import apache_beam as beam
from pipe_segment.pipelines.messages_segmented.bq_source import BigQuerySource


logger = logging.getLogger(__name__)


def build_filter(bq_source: BigQuerySource, start_date: date, end_date: date) -> str:
    """Returns the condition that keeps the rows of the source between the two dates.

    Args:
        bq_source:
            The source table of the messages.

        start_date:
            First day to keep, included.

        end_date:
            Last day to keep, included.

    Returns:
        The condition, to use in a ``WHERE`` clause.
    """
    return f"{bq_source.filtering_field} BETWEEN '{start_date}' AND '{end_date}'"


def build_days_query(sources: list, start_date: date, end_date: date) -> str:
    """Returns a query that counts the days with messages between the dates, in all sources.

    Args:
        sources:
            The :class:`BigQuerySource` of each table with messages.

        start_date:
            First day, included.

        end_date:
            Last day, included.

    Returns:
        The query.
    """
    selects = []
    for source in sources:
        selects.append(f"""
                SELECT
                  {source.filtering_field} AS day
                FROM `{source.table_id}`
                WHERE {build_filter(source, start_date, end_date)}
            """)

    union = "\nUNION ALL\n".join(selects)
    return f"SELECT COUNT(DISTINCT day) FROM ({union})"


class ReadMessages(beam.PTransform):
    def __init__(
        self,
        bq_helper,
        sources,
        start_date,
        end_date,
        labels,
        ssvid_filter_query=None,
    ):
        self.bq_helper = bq_helper
        self.sources = sources
        self.start_date = start_date
        self.end_date = end_date
        self.labels = labels
        self.ssvid_filter_query = ssvid_filter_query

    def build_query(self, source: str):
        bq_source = BigQuerySource(self.bq_helper, source)

        query = f"""
        SELECT *
        FROM (
            SELECT
              CAST(UNIX_MICROS(timestamp) AS FLOAT64) / 1000000  AS timestamp,
                * except (timestamp)
            FROM `{bq_source.table_id}`
            WHERE {build_filter(bq_source, self.start_date, self.end_date)}
        )
        """
        if self.ssvid_filter_query is not None:
            query = f"{query} WHERE ssvid IN ({self.ssvid_filter_query})"

        logger.debug(f"QUERY:\n{query}")

        return query

    def expand(self, pcoll):
        return [
            pcoll
            | f"ReadFromSrc{i}"
            >> beam.io.ReadFromBigQuery(
                query=self.build_query(src),
                use_standard_sql=True,
                bigquery_job_labels=self.labels,
            )
            for i, src in enumerate(self.sources)
        ] | beam.Flatten()
