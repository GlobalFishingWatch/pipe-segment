import apache_beam as beam


class ReadFragments(beam.PTransform):
    def __init__(
        self,
        source,
        start_date,
        end_date,
    ):
        self.source = source
        self.start_date = start_date
        self.end_date = end_date

    @property
    def query(self):
        query = f"""
        SELECT *
        FROM (
            SELECT
              CAST(UNIX_MILLIS(timestamp) AS FLOAT64) / 1000  AS timestamp,
              CAST(UNIX_MILLIS(first_msg_timestamp) AS FLOAT64) / 1000
                    AS first_msg_timestamp,
              CAST(UNIX_MILLIS(last_msg_timestamp) AS FLOAT64) / 1000
                    AS last_msg_timestamp,
                * except (timestamp, first_msg_timestamp, last_msg_timestamp)
            FROM `{self.source}*`
            WHERE _TABLE_SUFFIX BETWEEN 
             '{self.start_date:%Y%m%d}' AND '{self.end_date:%Y%m%d}'
            ORDER BY ssvid, timestamp
        )
        """
        return query

    def expand(self, pcoll):
        return pcoll | beam.io.ReadFromBigQuery(query=self.query, use_standard_sql=True)
