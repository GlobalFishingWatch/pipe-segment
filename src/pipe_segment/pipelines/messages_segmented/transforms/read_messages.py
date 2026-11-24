import logging

import apache_beam as beam
from pipe_segment.pipelines.messages_segmented.queries import MessagesQuery


logger = logging.getLogger(__name__)


class ReadMessages(beam.PTransform):
    def __init__(
        self,
        sources,
        start_date,
        end_date,
        labels,
        ssvid_filter_query=None,
    ):
        self.sources = sources
        self.start_date = start_date
        self.end_date = end_date
        self.labels = labels
        self.ssvid_filter_query = ssvid_filter_query

    def build_query(self, source: str):
        query = MessagesQuery(
            source=source,
            start_date=self.start_date,
            end_date=self.end_date,
            ssvid_filter_query=self.ssvid_filter_query,
        ).render()
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
