from datetime import timedelta
from jinja2 import Template
from importlib import resources
from typing import Any, Callable

import apache_beam as beam
from apache_beam import PTransform, io


class SatelliteOffsets(PTransform):
    """Generate satellite offset times based on Spire data.

    Example
    -------

        offsets = pipeline | SatelliteOffsets(start_date, end_date)
    """

    def __init__(
        self,
        source_table,
        norad_to_receiver_tbl,
        sat_positions_tbl,
        start_date,
        end_date,
        labels,
        read_from_bigquery_factory: Callable[..., Any] = io.ReadFromBigQuery,
    ):
        self.source_table = source_table
        self.norad_to_receiver_tbl = norad_to_receiver_tbl
        self.sat_positions_tbl = sat_positions_tbl
        self.start_date = start_date
        self.end_date = end_date
        self.labels = labels
        self.read_from_bigquery_factory = read_from_bigquery_factory

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
            yield self.read_from_bigquery_factory(
                query=query, use_standard_sql=True, bigquery_job_labels=self.labels
            )

    def _get_query_windows(self):
        start_date, end_date = self.start_date, self.end_date
        start_window = start_date
        while start_window <= end_date:
            end_window = min(start_window + timedelta(days=800), end_date)
            yield start_window, end_window
            start_window = end_window + timedelta(days=1)
