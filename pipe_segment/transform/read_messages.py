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
        WITH time_augmented AS (
            SELECT *,
                   TIMESTAMP(DATETIME_ADD(
                        PARSE_DATETIME("%Y%m%d", _table_suffix ),  
                        INTERVAL 1 DAY) AS midnight,
                   TIMESTAMP(DATETIME_SUB(DATETIME_ADD(
                        PARSE_DATETIME("%Y%m%d", _table_suffix ),  
                        INTERVAL 1 DAY), INTERVAL 1 MICROSECOND) AS almost_midnight,
            FROM `{source}*`
            WHERE _TABLE_SUFFIX BETWEEN 
             '{self.start_date:%Y%m%d}' AND '{self.end_date:%Y%m%d}'
        ),


        source_with_fixed_times AS (
            SELECT LEAST(timestamp, almost_midnight) AS timestamp,
                   * EXCEPT (timestamp, midnight, almost_midnight)
            FROM time_augmented
            WHERE timestamp <= midnight
        )

        SELECT *
        FROM (
            SELECT
              CAST(UNIX_MICROS(timestamp) AS FLOAT64) / 1000000  AS timestamp,
                * except (timestamp)
            FROM source_with_fixed_times
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
