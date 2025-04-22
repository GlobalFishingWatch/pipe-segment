from dataclasses import dataclass
from datetime import datetime

import apache_beam as beam

from pipe_segment.transform.satellite_offsets import SatelliteOffsets

from apache_beam.testing.test_pipeline import TestPipeline


class ReadFromBigQueryMock(beam.io.ReadFromBigQuery):
    def expand(self, pcoll):
        return pcoll | beam.Create(["dummy"])


@dataclass
class TableMock:
    description = "description"
    labels = []


@dataclass
class BigQueryClientMock:
    project: str = 'gfw'
    notfound: bool = False
    badrequest: bool = False

    def get_table(self, table):
        return TableMock()

    def update_table(self, table, fields):
        return table


class OptionsMock:
    date_range = '2024-01-01, 2024-01-01'
    in_normalized_sat_offset_messages_table = "project.dummy_table"
    in_norad_to_receiver_table = "project.dummy_table"
    in_sat_positions_table = "project.dummy_table"
    out_sat_offsets_table = "project.dummy_table"


class CloudOptionsMock:
    project = "mock"
    labels = []


def test_satellite_offsets(monkeypatch):
    # TODO: replace this monkey patch when design allows for more easy testing.
    monkeypatch.setattr(beam.io, "ReadFromBigQuery", ReadFromBigQueryMock)

    dummy_table = ""
    start_date = datetime(2024, 1, 1)
    end_date = datetime(2024, 1, 1)

    query_op = SatelliteOffsets(dummy_table, dummy_table, dummy_table, start_date, end_date)

    # messages = []
    with TestPipeline() as p:

        p | query_op
