import apache_beam as beam


class ReadMessages(beam.PTransform):
    def __init__(
        self,
        source,
        start_date,
        end_date,
        ssvid_filter_query,
    ):
        self.source = source
        self.start_date = start_date
        self.end_date = end_date
        self.ssvid_filter_query = ssvid_filter_query

    @property
    def query(self):
        query = f"""
        SELECT *
        FROM (
            SELECT
              CAST(UNIX_MILLIS(timestamp) AS FLOAT64) / 1000  AS timestamp,
                * except (timestamp)
            FROM `{self.source}*`
            WHERE _TABLE_SUFFIX BETWEEN 
             '{self.start_date:%Y%m%d}' AND '{self.end_date:%Y%m%d}'
            ORDER BY ssvid, timestamp
        )
        """
        if self.ssvid_filter_query is not None:
            query = f"{query} WHERE ssvid IN ({self.ssvid_filter_query})"
        return query

    def expand(self, pcoll):
        return pcoll | beam.io.ReadFromBigQuery(query=self.query, use_standard_sql=True)
