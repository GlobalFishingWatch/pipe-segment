from datetime import date, datetime

import apache_beam as beam
from apache_beam.testing.test_pipeline import TestPipeline
from pipe_segment.pipelines.messages_segmented.config import MessagesSegmentedConfig
from pipe_segment.pipelines.messages_segmented.transforms.satellite_offsets import (
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


def make_config():
    return MessagesSegmentedConfig(
        project="project",
        bq_in_normalized_messages="project.dummy_table",
        bq_out_segmented_messages="project.dummy_table",
        bq_out_segments="project.dummy_table",
        bq_out_fragments="project.dummy_table",
        bq_in_sat_offset_messages="project.dummy_table",
        bq_in_norad_to_receiver="project.dummy_table",
        bq_in_sat_positions="project.dummy_table",
        bq_out_sat_offsets="project.dummy_table",
        start_date=date(2024, 1, 1),
        end_date=date(2024, 1, 2),
        labels={"environment": "test"},
    )


def test_satellite_offsets(monkeypatch):
    # TODO: replace this monkey patch when design allows for more easy testing.
    monkeypatch.setattr(beam.io, "ReadFromBigQuery", ReadFromBigQueryMock)
    monkeypatch.setattr(beam.io, "WriteToBigQuery", ReadFromBigQueryMock)

    dummy_table = ""
    start_date = datetime(2024, 1, 1)
    end_date = datetime(2024, 1, 1)

    query_op = SatelliteOffsets(
        dummy_table, dummy_table, dummy_table, start_date, end_date, labels={}
    )
    write_op = SatelliteOffsetsWrite(make_config())

    # messages = []
    with TestPipeline() as p:

        p | query_op
        p | write_op


def test_satellite_offsets_labels_every_read(monkeypatch):
    read_calls = []

    def fake_read(**kwargs):
        read_calls.append(kwargs)
        return beam.Create([])

    monkeypatch.setattr(beam.io, "ReadFromBigQuery", fake_read)

    labels = {"environment": "production", "stage": "segment--v5-0-4"}
    # Over 800 days, so the offsets are read in two windows.
    query_op = SatelliteOffsets(
        "", "", "", datetime(2022, 1, 1), datetime(2024, 6, 1), labels=labels
    )

    _ = beam.Pipeline() | query_op

    assert [call["bigquery_job_labels"] for call in read_calls] == [labels, labels]


def test_satellite_offsets_reads_messages_from_a_partitioned_table(monkeypatch):
    read_calls = []

    def fake_read(**kwargs):
        read_calls.append(kwargs)
        return beam.Create([])

    monkeypatch.setattr(beam.io, "ReadFromBigQuery", fake_read)

    query_op = SatelliteOffsets(
        "p.d.messages",
        "p.d.norad",
        "p.d.positions_",
        datetime(2024, 1, 1),
        datetime(2024, 1, 31),
        labels={},
    )

    _ = beam.Pipeline() | query_op

    query = read_calls[0]["query"]
    assert "FROM `p.d.messages`" in query
    assert 'DATE(timestamp) BETWEEN "2024-01-01" AND "2024-01-31"' in query
    assert "`p.d.messages*`" not in query
    # The satellite positions table is still date sharded.
    assert "`p.d.positions_*`" in query
