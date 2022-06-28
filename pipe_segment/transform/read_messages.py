import logging

import apache_beam as beam


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
        WITH source_with_trimmed_times AS (
            SELECT LEAST(timestamp,
                   TIMESTAMP(FORMAT("%s-%s-%sT23:59:59.999999Z",SUBSTR(_table_suffix, 1, 4), 
                            SUBSTR(_table_suffix, 5, 2), SUBSTR(_table_suffix, 7, 2)))) AS timestamp,
                   * EXCEPT (timestamp)
            FROM `{source}*`
            WHERE _TABLE_SUFFIX BETWEEN 
             '{self.start_date:%Y%m%d}' AND '{self.end_date:%Y%m%d}'
        )

        SELECT *
        FROM (
            SELECT
              CAST(UNIX_MICROS(timestamp) AS FLOAT64) / 1000000  AS timestamp,
                * except (timestamp)
            FROM source_with_trimmed_times
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
