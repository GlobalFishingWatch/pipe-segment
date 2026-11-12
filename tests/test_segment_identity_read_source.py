import apache_beam as beam

from pipe_segment.segment_identity.read_source import ReadSource


def test_read_source_labels_its_read():
    labels = {"environment": "production", "stage": "segment--v5-0-4"}
    read_source = ReadSource(
        "bq://project:dataset.segments",
        "bq://project:dataset.fragments",
        1704067200,
        1704153600,
        labels,
    )

    read = read_source.read_source()

    assert isinstance(read, beam.io.ReadFromBigQuery)
    assert read._kwargs["bigquery_job_labels"] == labels
