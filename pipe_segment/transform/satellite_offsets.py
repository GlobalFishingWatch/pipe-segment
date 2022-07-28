from datetime import timedelta
from jinja2 import Template

import apache_beam as beam
import apache_beam.io.gcp.bigquery
from apache_beam import PTransform, io


def make_schema():
    schema = {"fields": []}

    def add_field(name, field_type, mode="REQUIRED"):
        schema["fields"].append(
            dict(
                name=name,
                type=field_type,
                mode=mode,
            )
        )

    add_field("hour", "timestamp")
    add_field("receiver", "STRING")
    add_field("dt", "FLOAT", "NULLABLE")
    add_field("pings", "INTEGER", "NULLABLE")
    add_field("avg_distance_from_sat_km", "FLOAT", "NULLABLE")
    add_field("med_dist_from_sat_km", "FLOAT", "NULLABLE")

    return schema


class SatelliteOffsets(PTransform):
    """Generate satellite offset times based on Spire data.

    Example
    -------

        offsets = pipesline | SatelliteOffsets(start_date, end_date)
    """

    schema = make_schema()

    def __init__(self, source_table, norad_to_receiver_tbl, sat_positions_tbl, 
                start_date, end_date):
        self.source_table = source_table
        self.norad_to_receiver_tbl = norad_to_receiver_tbl
        self.sat_positions_tbl = sat_positions_tbl
        self.start_date = start_date
        self.end_date = end_date

    def expand(self, xs):
        return [
            xs | "SatOffsets{}".format(ndx) >> src
            for (ndx, src) in enumerate(self._sat_offset_iter())
        ] | "MergeSatOffsets" >> beam.Flatten()

    def _sat_offset_iter(self):
        with open("assets/satellite_offsets.sql.j2") as f:
            template = Template(f.read())

        for start_window, end_window in self._get_query_windows():
            query = template.render(
                source=self.source_table,
                norad_to_receiver_tbl=self.norad_to_receiver_tbl,
                sat_positions_tbl=self.sat_positions_tbl,
                start_window=start_window,
                end_window=end_window
            )
            yield io.Read(
                io.gcp.bigquery.BigQuerySource(query=query, use_standard_sql=True)
            )

    def _get_query_windows(self):
        start_date, end_date = self.start_date, self.end_date
        start_window = start_date
        while start_window <= end_date:
            end_window = min(start_window + timedelta(days=800), end_date)
            yield start_window, end_window
            start_window = end_window + timedelta(days=1)
