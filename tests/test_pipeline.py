import pytest
import posixpath as pp
import newlinejson as nlj

from apache_beam.testing.test_pipeline import TestPipeline as _TestPipeline
# rename the class to prevent py.test from trying to collect TestPipeline as a unit test class

import apache_beam as beam
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to
from apache_beam.testing.util import open_shards
from apache_beam import Map
from apache_beam import GroupByKey
from apache_beam import FlatMap

import pipeline
from pipeline.transforms.segment import Segment
from pipeline.coders import JSONCoder
from pipeline.coders import Timestamp2DatetimeDoFn
from pipeline.coders import Datetime2TimestampDoFn


@pytest.mark.filterwarnings('ignore:Using fallback coder:UserWarning')
@pytest.mark.filterwarnings('ignore:The compiler package is deprecated and removed in Python 3.x.:DeprecationWarning')
@pytest.mark.filterwarnings('ignore:open_shards is experimental.:FutureWarning')
class TestPipeline():

    def _run_pipeline (self, source, messages_sink, segments_sink, expected, args=[]):
        args += [
            '--messages_source=%s' % source,
            '--messages_schema={"fields": []}',
            '--messages_sink=%s' % messages_sink,
            '--segments_sink=%s' % segments_sink,
            'local',
        ]

        pipeline.__main__.run(args, force_wait=True)

        with nlj.open(expected) as expected:
            with open_shards('%s*' % messages_sink) as output:
                assert sorted(expected) == sorted(nlj.load(output))

    def test_Pipeline_basic_args(self, test_data_dir, temp_dir):
        source = pp.join(test_data_dir, 'input.json')
        messages_sink = pp.join(temp_dir, 'messages')
        segments_sink = pp.join(temp_dir, 'segments')
        expected = pp.join(test_data_dir, 'expected_messages.json')

        self._run_pipeline(source, messages_sink, segments_sink, expected)

    def test_Pipeline_window(self, test_data_dir, temp_dir):
        source = pp.join(test_data_dir, 'input.json')
        messages_sink = pp.join(temp_dir, 'messages')
        segments_sink = pp.join(temp_dir, 'segments')
        expected = pp.join(test_data_dir, 'expected-window-1.json')
        args = [ '--window_size=1' ]
        self._run_pipeline(source, messages_sink, segments_sink, expected, args)

    def test_Pipeline_segmenter_params(self, test_data_dir, temp_dir):
        source = pp.join(test_data_dir, 'input.json')
        messages_sink = pp.join(temp_dir, 'messages')
        segments_sink = pp.join(temp_dir, 'segments')
        expected = pp.join(test_data_dir, 'expected_messages.json')
        segmenter_params = pp.join(test_data_dir, 'segmenter_params.json')
        args = [ '--segmenter_params=@%s' % segmenter_params]
        self._run_pipeline(source, messages_sink, segments_sink, expected, args)

    def test_Pipeline_parts(self, test_data_dir, temp_dir):
        source = pp.join(test_data_dir, 'input.json')
        messages_sink = pp.join(temp_dir, 'messages')
        segments_sink = pp.join(temp_dir, 'segments')
        expected_messages = pp.join(test_data_dir, 'expected_messages.json')
        expected_segments = pp.join(test_data_dir, 'expected_segments.json')

        with _TestPipeline() as p:
            segmented = (
                p
                | beam.io.ReadFromText(file_pattern=source, coder=JSONCoder())
                | "timestamp2datetime" >> beam.ParDo(Timestamp2DatetimeDoFn())
                | "ExtractMMSI" >> Map(lambda row: (row['mmsi'], row))
                | "GroupByMMSI" >> GroupByKey('mmsi')
                | Segment()
            )

            messages = segmented[Segment.OUTPUT_TAG_MESSAGES]
            (messages
                | "datetime2timestamp" >> beam.ParDo(Datetime2TimestampDoFn())
                | "WriteToMessagesSink" >> beam.io.WriteToText(
                    file_path_prefix=messages_sink,
                    num_shards=1,
                    coder=JSONCoder()
                )
            )

            segments = segmented[Segment.OUTPUT_TAG_SEGMENTS]
            (segments
                | "WriteToSegmentsSink" >> beam.io.WriteToText(
                    file_path_prefix=segments_sink,
                    num_shards=1,
                    coder=JSONCoder()
                )
            )

            p.run()
            with nlj.open(expected_messages) as expected:
                with open_shards('%s*' % messages_sink) as output:
                    assert sorted(expected) == sorted(nlj.load(output))

            with nlj.open(expected_segments) as expected:
                with open_shards('%s*' % segments_sink) as output:
                    assert sorted(expected) == sorted(nlj.load(output))
