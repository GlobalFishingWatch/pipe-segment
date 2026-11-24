from datetime import timedelta
from jinja2 import Template
from importlib import resources

import apache_beam as beam
from apache_beam import PTransform, io
from pipe_segment.pipelines.messages_segmented.config import MessagesSegmentedConfig


def make_schema():
    """Returns the schema fields array."""
    schema = []

    def add_field(name, field_type, mode="REQUIRED", description=""):
        schema.append(
            dict(
                name=name,
                type=field_type,
                mode=mode,
                description=description,
            )
        )

    add_field(
        "hour",
        "TIMESTAMP",
        description=(
            "Timestamp with hour resolution. "
            "This row applies starting from `hour` till `hour` plus sixty minutes.")
    )
    add_field("receiver", "STRING", description="The name of the satellite.")
    add_field(
        "dt",
        "FLOAT",
        "NULLABLE",
        description=(
            "How much the satellite clock is offset (in seconds)"
            "from the consensus (median) of all satellite clocks.")
        )
    add_field(
        "pings",
        "INTEGER",
        "NULLABLE",
        description=(
            "Number of pings used in computing the clock offset. "
            "Lower values mean the result is likely to be less reliable.")
        )
    add_field(
        "avg_distance_from_sat_km",
        "FLOAT",
        "NULLABLE",
        description=(
            "Average distance of vessels from the satellite during a given hour. "
            "Anomalously large values are another way to infer that the satellite clocks are off, "
            "however this has been superseded by the dt field.")
        )
    add_field(
        "med_dist_from_sat_km",
        "FLOAT",
        "NULLABLE",
        description=(
            "Median distance of vessels from the satellite during a given hour. "
            "Anomalously large values are another way to infer that the satellite clocks are off, "
            "however this has been superseded by the dt field.")
        )
    return schema


class SatelliteOffsets(PTransform):
    """Generate satellite offset times based on Spire data.

    Example
    -------

        offsets = pipeline | SatelliteOffsets(start_date, end_date)
    """

    def __init__(self, source_table, norad_to_receiver_tbl, sat_positions_tbl,
                 start_date, end_date, labels):
        self.source_table = source_table
        self.norad_to_receiver_tbl = norad_to_receiver_tbl
        self.sat_positions_tbl = sat_positions_tbl
        self.start_date = start_date
        self.end_date = end_date
        self.labels = labels

    def expand(self, xs):
        return [
            xs | f"SatOffsets{i}" >> src for (i, src) in enumerate(self._sat_offset_iter())
        ] | "MergeSatOffsets" >> beam.Flatten()

    def _sat_offset_iter(self):
        with resources.path('pipe_segment.assets', 'satellite_offsets.sql.j2') as t:
            with open(t) as f:
                template = Template(f.read())

        for start_window, end_window in self._get_query_windows():
            query = template.render(
                source=self.source_table,
                norad_to_receiver_tbl=self.norad_to_receiver_tbl,
                sat_positions_tbl=self.sat_positions_tbl,
                start_window=start_window,
                end_window=end_window
            )
            yield io.ReadFromBigQuery(
                query=query, use_standard_sql=True, bigquery_job_labels=self.labels
            )

    def _get_query_windows(self):
        start_date, end_date = self.start_date, self.end_date
        start_window = start_date
        while start_window <= end_date:
            end_window = min(start_window + timedelta(days=800), end_date)
            yield start_window, end_window
            start_window = end_window + timedelta(days=1)


class SatelliteOffsetsWrite(PTransform):
    """Writes the satellite offsets to BigQuery.

    Args:
        config:
            Configuration of the segment pipeline.
    """

    def __init__(self, config: MessagesSegmentedConfig):
        self.source_table = config.bq_in_sat_offset_messages
        self.source_norad = config.bq_in_norad_to_receiver
        self.source_sat_positions = config.bq_in_sat_positions
        self.dest_table = config.bq_out_sat_offsets
        self.schema = make_schema()

    def expand(self, xs):
        return xs | "WriteSatOffsets" >> io.WriteToBigQuery(
            self.dest_table,
            schema={"fields": self.schema},
            create_disposition=beam.io.BigQueryDisposition.CREATE_NEVER,
        )
