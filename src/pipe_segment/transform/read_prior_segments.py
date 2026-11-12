import logging

from datetime import date
from typing import Optional

import apache_beam as beam

from pipe_segment.utils.bq_tools import BigQueryHelper
from pipe_segment.utils.template_tools import format_query


logger = logging.getLogger(__name__)


class ReadPriorSegments(beam.PTransform):
    """Reads the state of the segments at the end of a day, keyed by ssvid.

    Args:
        bq_helper: helper to run queries and to label them.
        segments_table: table with the segments-days.
        fragments_table: table with the fragments.
        day: day to read the state from. It is the day before the first day to process.
        ssvid_filter_query: optional query that returns the ssvids to keep.
    """

    def __init__(
        self,
        bq_helper: BigQueryHelper,
        segments_table: str,
        fragments_table: str,
        day: date,
        ssvid_filter_query: Optional[str] = None,
    ):
        self.bq_helper = bq_helper
        self.segments_table = segments_table
        self.fragments_table = fragments_table
        self.day = day
        self.ssvid_filter_query = ssvid_filter_query

    def build_query(self) -> str:
        return format_query(
            "prior_segments.sql.j2",
            segments_table=self.segments_table,
            fragments_table=self.fragments_table,
            day=self.day,
            ssvid_filter_query=self.ssvid_filter_query,
        )

    def verify(self):
        """Fails if the segments table has no rows for the day.

        Starting from an empty state would create new segment ids for every open segment,
        so the run must stop instead.
        """
        count = self.bq_helper.fetch_scalar(
            f"SELECT COUNT(*) FROM `{self.segments_table}` WHERE DATE(timestamp) = '{self.day}'"
        )
        if count == 0:
            raise ValueError(
                f"Table {self.segments_table} has no segments on {self.day}. "
                "An incremental run needs the segments of the day before the date range. "
                "Run without --incremental to rebuild from the whole fragment history."
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
                bigquery_job_labels=self.bq_helper.labels,
                use_standard_sql=True,
            )
            | "KeyPriorSegmentsBySsvid" >> beam.Map(lambda row: (row["ssvid"], row))
        )
