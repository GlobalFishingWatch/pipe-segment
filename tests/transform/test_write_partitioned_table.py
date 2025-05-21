from datetime import datetime
import apache_beam as beam

from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that, equal_to

from pipe_segment.transform.write_partitioned_table import WritePartitionedTable


class WriteToBigQueryMock(beam.io.WriteToBigQuery):
    def __init__(self, table, *args, **kwargs):
        self._table = table
        super().__init__(table, *args, **kwargs)

    def expand(self, pcoll):
        return pcoll | beam.Map(lambda x: self._table)


def test_read_messages(monkeypatch):
    # TODO: replace this monkey patch when design allows for more easy testing.
    monkeypatch.setattr(beam.io, "WriteToBigQuery", WriteToBigQueryMock)

    table = "dummy_ds.dummy_table"
    schema = {}
    description = None
    partition_field = "timestamp"
    inputs = [{"timestamp": datetime(2024, 1, 1).timestamp()}]
    outputs = ["dummy_ds.dummy_table"]

    # Test without ssvid_filter_query
    op = WritePartitionedTable(table, schema, description, partition_field)
    with TestPipeline() as p:
        pcoll = p | beam.Create(inputs)
        output = pcoll | op
        assert_that(output, equal_to(outputs))
