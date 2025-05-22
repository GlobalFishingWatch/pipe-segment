from datetime import datetime
import apache_beam as beam

from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that, equal_to

from pipe_segment.transform.write_sink import WriteSink


class WriteToBigQueryMock(beam.io.WriteToBigQuery):
    def __init__(self, table, *args, **kwargs):
        self._table = table
        super().__init__(table, *args, **kwargs)

    def expand(self, pcoll):
        return pcoll | beam.Map(self._table)


def test_write_sink(monkeypatch):
    # TODO: replace this monkey patch when design allows for more easy testing.
    monkeypatch.setattr(beam.io, "WriteToBigQuery", WriteToBigQueryMock)

    sink_table = "dummy_table"
    schema = {}
    inputs = [{"timestamp": datetime(2024, 1, 1).timestamp()}]
    outputs = ["dummy_table20240101"]

    # Test without ssvid_filter_query
    op = WriteSink(sink_table, schema)
    with TestPipeline() as p:
        pcoll = p | beam.Create(inputs)
        output = pcoll | op
        assert_that(output, equal_to(outputs))
