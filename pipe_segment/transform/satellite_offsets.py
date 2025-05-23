from datetime import timedelta
from importlib import resources

from google.cloud import bigquery
from jinja2 import Template

import apache_beam as beam
from apache_beam import PTransform, io
from pipe_segment.utils.bq_tools import BigQueryHelper, DatePartitionedTable
from pipe_segment.version import __version__


def make_schema():
    schema = {"fields": []}

    def add_field(name, field_type, mode="REQUIRED", description=""):
        schema["fields"].append(
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
    return schema["fields"]


class SatelliteOffsets(PTransform):
    """Generate satellite offset times based on Spire data.

    Example
    -------

        offsets = pipeline | SatelliteOffsets(start_date, end_date)
    """

    def __init__(self, source_table, norad_to_receiver_tbl, sat_positions_tbl,
                 start_date, end_date):
        self.source_table = source_table
        self.norad_to_receiver_tbl = norad_to_receiver_tbl
        self.sat_positions_tbl = sat_positions_tbl
        self.start_date = start_date
        self.end_date = end_date

    def expand(self, xs):
        return [
            xs | f"SatOffsets{i}" >> src for (i, src) in enumerate(self._sat_offset_iter())
        ] | "MergeSatOffsets" >> beam.Flatten()

    def _sat_offset_iter(self):
        with resources.path(
            'pipe_segment.transform.assets', 'satellite_offsets.sql.j2'
        ) as template_filepath:
            with open(template_filepath) as f:
                template = Template(f.read())

        for start_window, end_window in self._get_query_windows():
            query = template.render(
                source=self.source_table,
                norad_to_receiver_tbl=self.norad_to_receiver_tbl,
                sat_positions_tbl=self.sat_positions_tbl,
                start_window=start_window,
                end_window=end_window
            )
            yield io.ReadFromBigQuery(query=query, use_standard_sql=True)

    def _get_query_windows(self):
        start_date, end_date = self.start_date, self.end_date
        start_window = start_date
        while start_window <= end_date:
            end_window = min(start_window + timedelta(days=800), end_date)
            yield start_window, end_window
            start_window = end_window + timedelta(days=1)


class SatelliteOffsetsWrite(PTransform):

    def __init__(self, options):
        self.source_table = options.in_normalized_sat_offset_messages_table
        self.source_norad = options.in_norad_to_receiver_table
        self.source_sat_positions = options.in_sat_positions_table
        self.dest_table = options.out_sat_offsets_table
        self.schema = make_schema()

    def expand(self, xs):
        return xs | "WriteSatOffsets" >> io.WriteToBigQuery(
            self.dest_table,
            schema={"fields": self.schema},
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
            create_disposition=beam.io.BigQueryDisposition.CREATE_NEVER,
        )

    @classmethod
    def prepare_output_tables(cls, options, cloud_options):
        bq_helper = BigQueryHelper(
            bq_client=bigquery.Client(project=cloud_options.project),
            labels=cloud_options.labels,
        )

        start_date, end_date = options.date_range.split(",")

        table = DatePartitionedTable(
            table_id=options.out_sat_offsets_table,
            description=f"""
                Created by pipe-segment: {__version__}
                * It identifies, at the hourly level,
                * how much time a given satellite's clock differs
                * from the median of all the other satellite's clocks.
                * https://github.com/GlobalFishingWatch/pipe-segment
                * Source Satellite: {options.in_normalized_sat_offset_messages_table}
                * Source Norad: {options.in_norad_to_receiver_table}
                * Source Satellite Positions: {options.in_sat_positions_table}
                * Date: {end_date}""",
            schema=make_schema(),
            partitioning_field="hour",
        )

        bq_helper.ensure_table_exists(table)
        bq_helper.run_query(query=table.clear_query(start_date, end_date))
