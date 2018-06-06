import pytest
import unittest
from copy import deepcopy
from datetime import datetime
import pytz
import posixpath as pp
import newlinejson as nlj
from collections import Counter

import apache_beam as beam

from apache_beam.testing.test_pipeline import TestPipeline as _TestPipeline
# rename the class to prevent py.test from trying to collect TestPipeline as a unit test class

from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to
from apache_beam.testing.util import BeamAssertException
from apache_beam.testing.util import open_shards

from pipe_tools.timestamp import timestampFromDatetime
from pipe_tools.timestamp import datetimeFromTimestamp
from pipe_tools.utils.timestamp import as_timestamp

from pipe_tools.coders import JSONDictCoder

from pipe_segment.transform import Segment
from pipe_segment.transform import NormalizeDoFn

from gpsdio.schema import datetime2str


# for use with apache_beam.testing.util.assert_that
# for pcollections that contain dicts
#
# for example
# assert_that(pcoll, contains(expected))
def contains(subset):
    subset = list(subset)

    def _contains(superset):
        sorted_subset = sorted(subset)
        sorted_superset = sorted(list(superset))
        for sub, super in zip(sorted_subset, sorted_superset):
            for k,v in sub.items():
                if super.get(k) != v:
                    raise BeamAssertException(
                        'Failed assert: %s does not contain %s\n'
                        'mismatch in key "%s"  %s != %s' %
                        (super, sub, k, super.get(k), v))
    return _contains


def list_contains(superset, subset):
    for super, sub in zip(superset, subset):
        for k,v in sub.items():
            if super.get(k) != v:
                raise BeamAssertException(
                    'Failed assert: %s does not contain %s\n'
                    'mismatch in key "%s"  %s != %s' %
                    (super, sub, k, super.get(k), v))
    return True

@pytest.mark.filterwarnings('ignore:Using fallback coder:UserWarning')
@pytest.mark.filterwarnings('ignore:The compiler package is deprecated and removed in Python 3.x.:DeprecationWarning')
@pytest.mark.filterwarnings('ignore:open_shards is experimental.:FutureWarning')
class TestTransforms():
    ts = timestampFromDatetime(datetime(2017, 1, 1, 0, 0, 0, tzinfo=pytz.UTC))

    @staticmethod
    def _seg_id(ssvid, ts):
        ts = datetimeFromTimestamp(ts)
        return '{}-{}'.format(ssvid, datetime2str(ts))

    @staticmethod
    def groupby_fn(msg):
        return (msg['ssvid'], msg)

    def _run_segment(self, messages_in, segments_in, temp_dir):
        messages_file = pp.join(temp_dir, '_run_segment', 'messages')
        segments_file = pp.join(temp_dir, '_run_segment', 'segments')

        with _TestPipeline() as p:
            messages = (
                p | 'CreateMessages' >> beam.Create(messages_in)
                | 'AddKeyMessages' >> beam.Map(self.groupby_fn)
                | "MessagesGroupByKey" >> beam.GroupByKey()
            )
            segments = (
                p | 'CreateSegments' >> beam.Create(segments_in)
                | 'AddKeySegments' >> beam.Map(self.groupby_fn)
                | "SegmentsGroupByKey" >> beam.GroupByKey()
            )
            segmented = (
                messages
                | "Segment" >> Segment(segments)
            )
            messages = segmented['messages']
            segments = segmented[Segment.OUTPUT_TAG_SEGMENTS]
            messages | "WriteMessages" >> beam.io.WriteToText(
                messages_file, coder=JSONDictCoder())
            segments | "WriteSegments" >> beam.io.WriteToText(
                segments_file, coder=JSONDictCoder())

            p.run()

            with open_shards('%s*' % messages_file) as output:
                messages = sorted(list(nlj.load(output)), key=lambda m: (m['ssvid'], m['timestamp']))
            with open_shards('%s*' % segments_file) as output:
                segments = list(nlj.load(output))

            assert list_contains(messages, messages_in)

            return messages, segments

    def test_segment_empty(self, temp_dir):
        self._run_segment([], [], temp_dir=temp_dir)

    def test_segment_single(self, temp_dir):
        messages_in = [{'ssvid': 1, 'timestamp': self.ts}]
        segments_in = []
        messages_out, segments_out = self._run_segment(messages_in, segments_in, temp_dir=temp_dir)

    def test_segment_segments_in(self, temp_dir):
        prev_ts = self.ts - 1
        messages_in = [{'ssvid': "1", 'timestamp': self.ts}]
        segments_in = [{'ssvid': "1", 'timestamp': prev_ts,
                     'seg_id': self._seg_id(1, prev_ts),
                     'origin_ts': prev_ts,
                     'timestamp_last': self.ts,
                     'noise': False,
                     'last_pos_lat': 0,
                     'last_pos_lon': 0,
                     'message_count': 1}]
        messages_out, segments_out = self._run_segment(messages_in, segments_in, temp_dir=temp_dir)
        assert messages_out[0]['seg_id'] == segments_in[0]['seg_id']

    def test_segment_out_in(self, temp_dir):
        prev_ts = self.ts - 1
        messages_in = [{'ssvid': "1", 'timestamp': self.ts-1},
                       {'ssvid': "2", 'timestamp': self.ts-1}]
        segments_in = []
        messages_out, segments_out = self._run_segment(messages_in, segments_in, temp_dir=temp_dir)
        messages_in = [{'ssvid': "1", 'timestamp': self.ts},
                       {'ssvid': "2", 'timestamp': self.ts}]
        segments_in = segments_out
        messages_out, segments_out = self._run_segment(messages_in, segments_in, temp_dir=temp_dir)

        assert len(segments_out) == 2
        assert all(seg['message_count'] == 2 for seg in segments_out)
        assert all(seg['seg_id'] == self._seg_id(seg['ssvid'], prev_ts) for seg in segments_out)


    @pytest.mark.parametrize("message, expected", [
        ({}, {}),
        ({'shipname': 'f/v boaty Mc Boatface'}, {'n_shipname': 'BOATYMCBOATFACE'}),
        ({'callsign': '@@123'}, {'n_callsign': '123'}),
        ({'imo': 8814275}, {'n_imo': 8814275}),
    ])
    def test_normalize(self, message, expected):
        normalize = NormalizeDoFn()
        assert list_contains(list(normalize.process(message)), [expected])

    def test_normalize_invalid_imo(self):
        normalize = NormalizeDoFn()
        assert all ('n_imo' not in m for m in list(normalize.process({'imo': 0000000})))

    def test_noise_segment(self, temp_dir):
        messages_in = [
            {"timestamp": as_timestamp("2017-07-20T05:59:35.000000Z"),
             "ssvid": "338013000",
             "lon": -161.3321333333,
             "lat": -9.52616,
             "speed": 11.1},
            {"timestamp": as_timestamp("2017-07-20T06:00:38.000000Z"),
             "ssvid": "338013000",
             "lon": -161.6153106689,
             "lat": -9.6753702164,
             "speed": 11.3999996185},
            {"timestamp": as_timestamp("2017-07-20T06:01:00.000000Z"),
             "ssvid": "338013000"}
        ]

        segments_in = []
        messages_out, segments_out = self._run_segment(messages_in, segments_in, temp_dir=temp_dir)

        seg_stats = {(seg['seg_id'], seg['message_count'], seg['noise']) for seg in segments_out}

        assert seg_stats == {('338013000-2017-07-20T05:59:35.000000Z', 2, False),
                             ('338013000-2017-07-20T06:00:38.000000Z', 1, True)}

        messages_in = [{"timestamp": as_timestamp("2017-07-20T06:02:00.000000Z"),
             "ssvid": "338013000"}
        ]
        segments_in = segments_out
        messages_out, segments_out = self._run_segment(messages_in, segments_in, temp_dir=temp_dir)

        seg_stats = {(seg['seg_id'], seg['message_count'], seg['noise']) for seg in segments_out}

        assert seg_stats == {('338013000-2017-07-20T05:59:35.000000Z', 3, False)}


    def test_expected_segments(self, temp_dir):
        messages_in = [
            {"timestamp": as_timestamp("2017-11-15T11:14:32.000000Z"),
             "ssvid": 257666800,
             "lon": 5.3108466667,
             "lat": 60.40065,
             "speed": 6.5},
            {"timestamp": as_timestamp("2017-11-26T11:20:16.000000Z"),
             "ssvid": 257666800,
             "lon": 5.32334,
             "lat": 60.396235,
             "speed": 3.2000000477},
        ]

        segments_in = []
        messages_out, segments_out = self._run_segment(messages_in, segments_in, temp_dir=temp_dir)
        seg_stats = [(seg['seg_id'], seg['message_count'], seg['noise']) for seg in segments_out]

        expected = [('257666800-2017-11-15T11:14:32.000000Z', 1, False),
                    ('257666800-2017-11-26T11:20:16.000000Z', 1, False)]
        assert seg_stats == expected


