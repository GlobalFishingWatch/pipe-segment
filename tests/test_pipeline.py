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

from pipe_tools.coders import JSONDictCoder

from pipe_segment.__main__ import run as  pipe_segment_run
from pipe_segment.transform.segment import Segment
from pipe_segment.pipeline import SegmentPipeline


@pytest.mark.filterwarnings('ignore:Using fallback coder:UserWarning')
@pytest.mark.filterwarnings('ignore:The compiler package is deprecated and removed in Python 3.x.:DeprecationWarning')
@pytest.mark.filterwarnings('ignore:open_shards is experimental.:FutureWarning')
class TestPipeline():

    def _run_pipeline (self, source, messages_sink, segments_sink, expected, args=[]):
        args += [
            '--source=%s' % source,
            '--source_schema={"fields": []}',
            '--msg_dest=%s' % messages_sink,
            '--seg_dest=%s' % segments_sink,
            '--runner=DirectRunner',
            '--wait_for_job'
        ]

        pipe_segment_run(args)

        def listify(seq):
            return sorted([sorted(x.items()) for x in seq])

        with nlj.open(expected) as expected:
            with open_shards('%s*' % messages_sink) as raw_output:
                output = list(nlj.load(raw_output))
                assert listify(expected) == listify(output)

    def test_Pipeline_basic_args(self, test_data_dir, temp_dir):
        source = pp.join(test_data_dir, 'input.json')
        messages_sink = pp.join(temp_dir, 'messages')
        segments_sink = pp.join(temp_dir, 'segments')
        expected = pp.join(test_data_dir, 'expected_messages.json')

        self._run_pipeline(source, messages_sink, segments_sink, expected)

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
            messages = (
                p
                | beam.io.ReadFromText(file_pattern=source, coder=JSONDictCoder())
                | "MessagesAddKey" >> beam.Map(SegmentPipeline.groupby_fn)
            )
            segments = p | beam.Create([])

            args = (
                {'messages' : messages, 'segments' : segments}
            |    'GroupByKey' >> beam.CoGroupByKey()
            )

            segmentizer = Segment(look_ahead=0)

            segmented = args | segmentizer

            messages = segmented[segmentizer.OUTPUT_TAG_MESSAGES]
            (messages
                | "WriteToMessagesSink" >> beam.io.WriteToText(
                    file_path_prefix=messages_sink,
                    num_shards=1,
                    coder=JSONDictCoder()
                )
            )

            segments = segmented[segmentizer.OUTPUT_TAG_SEGMENTS]
            (segments
                | "WriteToSegmentsSink" >> beam.io.WriteToText(
                    file_path_prefix=segments_sink,
                    num_shards=1,
                    coder=JSONDictCoder()
                )
            )

            def listify(seq):
                return sorted([sorted(x.items()) for x in seq])

            p.run()

            with nlj.open(expected_messages) as raw_expected:
                expected = listify(raw_expected)
                with open_shards('%s*' % messages_sink) as output:
                    raw_actual = list(nlj.load(output))
                    actual = listify(raw_actual)
                    assert expected == actual

            with nlj.open(expected_segments) as expected_output:
                expected = listify(expected_output)
                with open_shards('%s*' % segments_sink) as actual_output:
                    raw_actual = list(nlj.load(actual_output))
                    actual = listify(raw_actual)
                    assert expected == actual


    def test_Pipeline_multiple_source(self, test_data_dir, temp_dir):
        source1 = pp.join(test_data_dir, 'input.json')
        source2 = pp.join(test_data_dir, 'input2.json')
        source = ",".join([source1, source2])
        messages_sink = pp.join(temp_dir, 'messages')
        segments_sink = pp.join(temp_dir, 'segments')
        expected = pp.join(test_data_dir, 'expected_messages2.json')
        self._run_pipeline(source, messages_sink, segments_sink, expected)

    def test_Pipeline_ident_messages(self, test_data_dir, temp_dir):
        source = pp.join(test_data_dir, 'ident-messages-in.json')
        messages_sink = pp.join(temp_dir, 'messages')
        segments_sink = pp.join(temp_dir, 'segments')
        expected = pp.join(test_data_dir, 'ident-messages-expected.json')
        self._run_pipeline(source, messages_sink, segments_sink, expected)
