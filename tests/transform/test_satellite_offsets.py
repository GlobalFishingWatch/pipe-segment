from datetime import datetime

import apache_beam as beam
from apache_beam.testing.test_pipeline import TestPipeline
from pipe_segment.transform.satellite_offsets import (
    SatelliteOffsets,
    SatelliteOffsetsWrite,
)


class ReadFromBigQueryMock(beam.io.ReadFromBigQuery):
    def expand(self, pcoll):
        return pcoll | beam.Create(["dummy"])


class WriteToBigQueryMock(beam.io.WriteToBigQuery):
    def __init__(self, table, *args, **kwargs):
        self._table = table
        super().__init__(table, *args, **kwargs)

    def expand(self, pcoll):
        return pcoll | beam.Map(self._table)


class OptionsMock:
    date_range = '2024-01-01, 2024-01-01'
    in_normalized_sat_offset_messages_table = "project.dummy_table"
    in_norad_to_receiver_table = "project.dummy_table"
    in_sat_positions_table = "project.dummy_table"
    out_sat_offsets_table = "project.dummy_table"


def test_satellite_offsets(monkeypatch):
    # TODO: replace this monkey patch when design allows for more easy testing.
    monkeypatch.setattr(beam.io, "ReadFromBigQuery", ReadFromBigQueryMock)
    monkeypatch.setattr(beam.io, "WriteToBigQuery", ReadFromBigQueryMock)

    dummy_table = ""
    start_date = datetime(2024, 1, 1)
    end_date = datetime(2024, 1, 1)

    query_op = SatelliteOffsets(dummy_table, dummy_table, dummy_table, start_date, end_date)
    write_op = SatelliteOffsetsWrite(OptionsMock())

    # messages = []
    with TestPipeline() as p:

        p | query_op
        p | write_op
