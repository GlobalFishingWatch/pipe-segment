import apache_beam as beam

SOURCE_QUERY_TEMPLATE = """
    SELECT
      seg.* EXCEPT(cumulative_identities, cumulative_destinations),
      frag.first_msg_timestamp, frag.last_msg_timestamp,
    FROM
      `{source_segments}` seg
    JOIN
      `{source_fragments}` frag
    USING (frag_id)
    WHERE
      (DATE(seg.timestamp) BETWEEN
        DATE(TIMESTAMP_SECONDS({start_ts})) AND DATE(TIMESTAMP_SECONDS({end_ts})))
      AND (DATE(frag.timestamp) BETWEEN
        DATE(TIMESTAMP_SECONDS({start_ts})) AND DATE(TIMESTAMP_SECONDS({end_ts})))
      AND TRUE
"""


class ReadSource(beam.PTransform):
    def __init__(self, source_segments, source_fragments, start_ts, end_ts):
        self.source_segments = source_segments.replace("bq://", "").replace(":", ".")
        self.source_fragments = source_fragments.replace("bq://", "").replace(":", ".")
        self.start_ts = int(start_ts)
        self.end_ts = int(end_ts)

    def read_source(self):
        query = SOURCE_QUERY_TEMPLATE.format(
            source_segments=self.source_segments,
            source_fragments=self.source_fragments,
            start_ts=self.start_ts,
            end_ts=self.end_ts,
        )
        return beam.io.ReadFromBigQuery(
            query=query,
            use_standard_sql=True,
        )

    def expand(self, pcoll):
        return pcoll | self.read_source()
