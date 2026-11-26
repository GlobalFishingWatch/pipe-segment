"""Core transform of the segment-identity-daily pipeline."""

import apache_beam as beam

from apache_beam.pvalue import PCollection

from pipe_segment.pipelines.segment_identity_daily.transforms.identities import (
    rename_timestamp,
    summarize_identifiers,
)


class SummarizeSegmentIdentity(beam.PTransform):
    """Summarizes the identity of each segment of a day, one row per segment and day."""

    def expand(self, pcoll: PCollection) -> PCollection:
        """Summarizes the identities of the segments, and names their day summary_timestamp."""
        return (
            pcoll
            | "SummarizeIdentifiers" >> beam.Map(summarize_identifiers)
            | "RenameTimestamp" >> beam.Map(rename_timestamp)
        )
