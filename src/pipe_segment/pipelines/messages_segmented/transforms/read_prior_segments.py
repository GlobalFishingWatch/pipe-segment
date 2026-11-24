import logging

from datetime import date
from typing import Optional

import apache_beam as beam

from pipe_segment.utils.template_tools import format_query


logger = logging.getLogger(__name__)


class ReadPriorSegments(beam.PTransform):
    """Reads the state of the segments at the end of a day, keyed by ssvid.

    Args:
        segments_table:
            Table with the segments-days.

        fragments_table:
            Table with the fragments.

        day:
            Day to read the state from. It is the day before the first day to process.

        ssvid_filter_query:
            Optional query that returns the ssvids to keep.

        read_from_bigquery_kwargs:
            Extra arguments for Beam's ``ReadFromBigQuery``, for example
            the labels of the query job.
    """

    def __init__(
        self,
        segments_table: str,
        fragments_table: str,
        day: date,
        ssvid_filter_query: Optional[str] = None,
        read_from_bigquery_kwargs: Optional[dict] = None,
    ):
        self.segments_table = segments_table
        self.fragments_table = fragments_table
        self.day = day
        self.ssvid_filter_query = ssvid_filter_query
        self.read_from_bigquery_kwargs = read_from_bigquery_kwargs or {}

    def build_query(self) -> str:
        return format_query(
            "prior_segments.sql.j2",
            segments_table=self.segments_table,
            fragments_table=self.fragments_table,
            day=self.day,
            ssvid_filter_query=self.ssvid_filter_query,
        )

    def expand(self, pcoll):
        query = self.build_query()
        logger.debug(f"Emitting prior segments query:\n{query}")
        return (
            pcoll
            | "ReadPriorSegments"
            >> beam.io.ReadFromBigQuery(
                query=query,
                method=beam.io.ReadFromBigQuery.Method.EXPORT,
                use_standard_sql=True,
                **self.read_from_bigquery_kwargs,
            )
            | "KeyPriorSegmentsBySsvid" >> beam.Map(lambda row: (row["ssvid"], row))
        )
