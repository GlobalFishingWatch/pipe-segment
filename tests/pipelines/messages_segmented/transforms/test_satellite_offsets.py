from datetime import datetime

import apache_beam as beam
from pipe_segment.pipelines.messages_segmented.transforms.satellite_offsets import (
    SatelliteOffsets,
)


def test_satellite_offsets_labels_every_read():
    """Checks that every window read of the satellite offsets carries the job labels."""
    read_calls = []

    def fake_read(**kwargs):
        read_calls.append(kwargs)
        return beam.Create([])

    labels = {"environment": "production", "stage": "segment--v5-0-4"}
    # Over 800 days, so the offsets are read in two windows.
    query_op = SatelliteOffsets(
        "", "", "", datetime(2022, 1, 1), datetime(2024, 6, 1), labels=labels,
        read_from_bigquery_factory=fake_read,
    )

    _ = beam.Pipeline() | query_op

    assert [call["bigquery_job_labels"] for call in read_calls] == [labels, labels]


def test_satellite_offsets_reads_messages_from_a_partitioned_table():
    """Checks that the offsets query reads the messages table by date, with no wildcard."""
    read_calls = []

    def fake_read(**kwargs):
        read_calls.append(kwargs)
        return beam.Create([])

    query_op = SatelliteOffsets(
        "p.d.messages",
        "p.d.norad",
        "p.d.positions_",
        datetime(2024, 1, 1),
        datetime(2024, 1, 31),
        labels={},
        read_from_bigquery_factory=fake_read,
    )

    _ = beam.Pipeline() | query_op

    query = read_calls[0]["query"]
    assert "FROM `p.d.messages`" in query
    assert 'DATE(timestamp) BETWEEN "2024-01-01" AND "2024-01-31"' in query
    assert "`p.d.messages*`" not in query
    # The satellite positions table is still date sharded.
    assert "`p.d.positions_*`" in query
