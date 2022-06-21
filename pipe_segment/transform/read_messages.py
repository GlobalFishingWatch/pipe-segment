import apache_beam as beam
import logging


class ReadMessages(beam.PTransform):
    def __init__(
        self,
        sources,
        start_date,
        end_date,
        ssvid_filter_query,
    ):
        self.sources = sources
        self.start_date = start_date
        self.end_date = end_date
        self.ssvid_filter_query = ssvid_filter_query

    def query(self, source):
        query = f"""
        SELECT *
        FROM (
            SELECT
              CAST(UNIX_MILLIS(timestamp) AS FLOAT64) / 1000  AS timestamp,
                * except (timestamp)
            FROM `{source}*`
            WHERE _TABLE_SUFFIX BETWEEN 
             '{self.start_date:%Y%m%d}' AND '{self.end_date:%Y%m%d}'
        )
        """
        if self.ssvid_filter_query is not None:
            query = f"{query} WHERE ssvid IN ({self.ssvid_filter_query})"
        logging.info(f"QUERY:\n{query}")
        return query

    def expand(self, pcoll):
        return (
            pcoll
            | f"ReadFromSrc{i}"
            >> beam.io.ReadFromBigQuery(query=self.query(src), use_standard_sql=True)
            for i, src in enumerate(self.sources)
        ) | beam.Flatten()
