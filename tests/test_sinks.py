import pytest
from datetime import datetime

import apache_beam as beam
from apache_beam import io
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to
from apache_beam.testing.test_pipeline import TestPipeline as _TestPipeline
# rename the class to prevent py.test from trying to collect TestPipeline as a unit test class

from pipeline.coders import timestamp2datetime
from pipeline.coders import datetime2timestamp
from pipeline.coders import JSONCoder
from pipeline.transforms import sink


@pytest.mark.filterwarnings('ignore:Using fallback coder:UserWarning')
@pytest.mark.filterwarnings('ignore:The compiler package is deprecated and removed in Python 3.x.:DeprecationWarning')
class TestBigQuerySink():
    SCHEMA = "mmsi:INTEGER,timestamp:TIMESTAMP"

    def _sample_data(self):
        start_ts = datetime2timestamp(datetime(2017, 1, 1, 0, 0, 0))
        increment = 100.0
        count = 864 * 2
        ts = start_ts
        for t in xrange(count):
            yield {'mmsi': 1, 'timestamp': ts}
            ts += increment

    @pytest.mark.skip(reason="this runs against bigquery.  For debugging only for now.")
    def test_partitioned(self):
        def _partition_fn(msg):
            return timestamp2datetime(msg['timestamp']).strftime('%Y%m%d')


        with _TestPipeline() as p:
            messages = (
                p
                | beam.Create(self._sample_data())
            )
            (
                messages
                | sink.WriteToDatePartitionedBigQuery(table='world-fishing-827:scratch_paul.shard_test_a',
                                                      schema=self.SCHEMA,
                                                      partition_fn=_partition_fn)
                # | beam.Map(lambda row: (_partition_fn(row), row))
                # | "GroupByPartition" >> beam.GroupByKey('partition')
                # | io.Write(sink.ShardedBigQuerySink(table='shard_test_a',
                #                                     schema = self.SCHEMA))
            )
            # partitioned = (
            #     messages
            #     | beam.Map(lambda row: (_partition_fn(row), row))
            #     | "GroupByPartition" >> beam.GroupByKey('partition')
            # )
            # (partitioned
            #     | "WriteToMessagesSink" >> beam.io.WriteToText(
            #         file_path_prefix='./partition-test',
            #         num_shards=1,
            #         coder=JSONCoder()
            #     )
            # )
        # p.run()
        # assert_that(messages, equal_to(self.SAMPLE_DATA))

