import logging

import apache_beam as beam
from pipe_segment.pipelines.messages_segmented.queries import MessagesQuery


logger = logging.getLogger(__name__)


class ReadMessages(beam.PTransform):
    """Reads the messages of a source table between two days."""

    def __init__(
        self,
        source,
        start_date,
        end_date,
        labels,
        ssvid_filter_query=None,
    ):
        self.source = source
        self.start_date = start_date
        self.end_date = end_date
        self.labels = labels
        self.ssvid_filter_query = ssvid_filter_query

    def build_query(self) -> str:
        query = MessagesQuery(
            source=self.source,
            start_date=self.start_date,
            end_date=self.end_date,
            ssvid_filter_query=self.ssvid_filter_query,
        ).render()
        logger.debug(f"QUERY:\n{query}")

        return query

    def expand(self, pcoll):
        return pcoll | "ReadMessages" >> beam.io.ReadFromBigQuery(
            query=self.build_query(),
            use_standard_sql=True,
            bigquery_job_labels=self.labels,
        )
