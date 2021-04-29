# rename the class to prevent py.test from trying to collect TestPipeline as a unit test class
from apache_beam.testing.test_pipeline import TestPipeline as _TestPipeline
from apache_beam.testing.util import BeamAssertException
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to
from apache_beam.testing.util import open_shards

from collections import Counter

from copy import deepcopy

from datetime import datetime

from pipe_segment.transform.normalize import NormalizeDoFn
from pipe_segment.transform.segment import Segment
from pipe_segment.options.segment import SegmentOptions

from pipe_tools.coders import JSONDictCoder
from pipe_tools.timestamp import datetimeFromTimestamp
from pipe_tools.timestamp import timestampFromDatetime
from pipe_tools.utils.timestamp import as_timestamp

import apache_beam as beam
import newlinejson as nlj
import posixpath as pp
import pytest
import pytz
import unittest

# >>> Note that cogroupByKey treats unicode and char values as distinct,
# so tests can sometimes fail unless all ssvid are unicode.

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
        return '{}-{:%Y-%m-%dT%H:%M:%S.%fZ}'.format(ssvid, ts)

    @staticmethod
    def groupby_fn(msg):
        return (msg['ssvid'], msg)

    def _run_segment(self, messages_in, segments_in, temp_dir):
        messages_file = pp.join(temp_dir, '_run_segment', 'messages')
        segments_file = pp.join(temp_dir, '_run_segment', 'segments')

        args = [
           f'--source={messages_in}',
           f'--msg_dest={messages_file}',
           f'--seg_dest={segments_file}'
        ]
        segop = SegmentOptions(args)

        with _TestPipeline(options=segop) as p:
            messages = (
                p | 'CreateMessages' >> beam.Create(messages_in)
                | 'AddKeyMessages' >> beam.Map(self.groupby_fn)
            )
            segments = (
                p | 'CreateSegments' >> beam.Create(segments_in)
                | 'AddKeySegments' >> beam.Map(self.groupby_fn)
            )
            args = (
                {'messages' : messages, 'segments' : segments}
                | 'GroupByKey' >> beam.CoGroupByKey()
            )

            segmentizer = Segment()

            segmented = (
                args
                | "Segment" >> segmentizer
            )
            messages = segmented['messages']
            segments = segmented[segmentizer.OUTPUT_TAG_SEGMENTS]
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
        messages_in = [{'ssvid': 1, 'timestamp': self.ts, 'type': 'AIS.1'}]
        segments_in = []
        messages_out, segments_out = self._run_segment(messages_in, segments_in, temp_dir=temp_dir)

    def test_segment_segments_in(self, temp_dir):
        prev_ts = self.ts - 1
        messages_in = [{'ssvid': "1", 'timestamp': self.ts, 'type' : 'AIS.1'}]
        segments_in = [{'ssvid': "1", 
                     'seg_id': self._seg_id("1", prev_ts),
                     'timestamp' : prev_ts,
                     'closed' : False,
                     'first_msg_lat': 0,
                     'first_msg_lon': 0,
                     'first_msg_course' : 0,
                     'first_msg_speed' : 0,
                     'first_msg_timestamp' : prev_ts,
                     'last_msg_lat': 0,
                     'last_msg_lon': 0,
                     'last_msg_course' : 0,
                     'last_msg_speed' : 0,
                     'last_msg_timestamp' : prev_ts,
                     'first_msg_of_day_lat': 0,
                     'first_msg_of_day_lon': 0,
                     'first_msg_of_day_course' : 0,
                     'first_msg_of_day_speed' : 0,
                     'first_msg_of_day_timestamp' : prev_ts,
                     'last_msg_of_day_lat': 0,
                     'last_msg_of_day_lon': 0,
                     'last_msg_of_day_course' : 0,
                     'last_msg_of_day_speed' : 0,
                     'last_msg_of_day_timestamp' : prev_ts,
                     'message_count': 1,
                     'shipnames' : [],
                     'callsigns' : [],
                     'imos' : [],
                     'transponders' : []}]
        messages_out, segments_out = self._run_segment(messages_in, segments_in, temp_dir=temp_dir)
        assert messages_out[0]['seg_id'] == None # No longer assign seg ids to noise segments

    def test_segment_out_in(self, temp_dir):
        prev_ts = self.ts - 1
        messages_in = [{'ssvid': u"1", 'timestamp': self.ts-1, 
                        'lat' : 5.0, 'lon' : 0.1, 'speed' : 0.0, 'course' : 0.0,
                        'type' : 'AIS.1'},
                       {'ssvid': u"2", 'timestamp': self.ts-1, 
                        'lat' : 5.0, 'lon' : 0.1, 'speed' : 0.0, 'course' : 0.0,
                        'type' : 'AIS.1'}]
        segments_in = []
        messages_out, segments_out = self._run_segment(messages_in, segments_in, temp_dir=temp_dir)
        messages_in = [{'ssvid': u"1", 'timestamp': self.ts, 
                        'lat' : 5.0, 'lon' : 0.1, 'speed' : 0.0, 'course' : 0.0,
                        'type' : 'AIS.1'},
                       {'ssvid': u"2", 'timestamp': self.ts, 
                        'lat' : 5.0, 'lon' : 0.1, 'speed' : 0.0, 'course' : 0.0,
                        'type' : 'AIS.1'}]
        segments_in = segments_out
        messages_out, segments_out = self._run_segment(messages_in, segments_in, temp_dir=temp_dir)

        assert len(segments_out) == 2
        assert all(seg['message_count'] == 2 for seg in segments_out)
        assert all(seg['seg_id'] == self._seg_id(seg['ssvid'], prev_ts) for seg in segments_out)


    @pytest.mark.parametrize("message, expected", [
        ({}, {}),
        ({'shipname': 'f/v boaty Mc Boatface'}, {'n_shipname': 'BOATYMCBOATFACE'}),
        ({'shipname': 'Bouy 42%'}, {'n_shipname': 'BOUY42'}),
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
             "msgid" : 0,
             "ssvid": u"338013000",
             "lon": -161.3321333333,
             "lat": -9.52616,
             "speed": 11.1,
             'course' : 0.0,
             'type' : 'AIS.1'},
            {"timestamp": as_timestamp("2017-07-20T06:00:38.000000Z"),
             "msgid" : 1,
             "ssvid": u"338013000",
             "lon": -161.6153106689,
             "lat": -9.6753702164,
             'course' : 0.0,
             "speed": 11.3999996185,
             'type' : 'AIS.1'},
            {"timestamp": as_timestamp("2017-07-20T06:01:00.000000Z"),
             "msgid" : 2,
             "ssvid": u"338013000",
             'type' : 'AIS.1'}
        ]

        segments_in = []
        messages_out, segments_out = self._run_segment(messages_in, segments_in, temp_dir=temp_dir)

        seg_stats = {(seg['seg_id'], seg['message_count']) for seg in segments_out}

        assert seg_stats == {(u'338013000-2017-07-20T05:59:35.000000Z', 1),
                             (u'338013000-2017-07-20T06:00:38.000000Z', 1)}

        messages_in = [{"timestamp": as_timestamp("2017-07-20T06:02:00.000000Z"),
             "ssvid": u"338013000", 'type' : 'AIS.1'}
        ]
        segments_in = segments_out
        messages_out, segments_out = self._run_segment(messages_in, segments_in, temp_dir=temp_dir)

        seg_stats = {(seg['seg_id'], seg['message_count'], seg['closed']) for seg in segments_out}

        assert seg_stats == {
                        ('338013000-2017-07-20T05:59:35.000000Z', 1, False),
                        ('338013000-2017-07-20T06:00:38.000000Z', 1, False)}


    def test_expected_segments(self, temp_dir):
        messages_in = [
            {"timestamp": as_timestamp("2017-11-15T11:14:32.000000Z"),
             "ssvid": 257666800,
             "lon": 5.3108466667,
             "lat": 60.40065,
             "speed": 0.0,
             'course' : 0.0,
             'type' : 'AIS.1'},
            {"timestamp": as_timestamp("2017-11-26T11:20:16.000000Z"),
             "ssvid": 257666800,
             "lon": 5.32334,
             "lat": 60.396235,
             "speed": 0.0,
             'course' : 0.0,
             'type' : 'AIS.1'},
        ]

        segments_in = []
        messages_out, segments_out = self._run_segment(messages_in, segments_in, temp_dir=temp_dir)
        seg_stats = set([(seg['seg_id'], seg['message_count']) for seg in segments_out])
        print(segments_out)
        expected = {('257666800-2017-11-15T11:14:32.000000Z', 1),
                    ('257666800-2017-11-26T11:20:16.000000Z', 1)}
        assert seg_stats == expected


    def test_message_type(self, temp_dir):
        messages_in = [
            {"timestamp": as_timestamp("2018-01-01 00:00"),
             "msgid" : 0,
             "ssvid": "123456789",
             "type": "AIS.1",
             "lon": 0.0,
             "lat": 0.0,
             'course' : 0.0,
             'speed' : 0.001},
            {"timestamp": as_timestamp("2018-01-01 01:00"),
             "msgid" : 1,
             "ssvid": "123456789",
             "type": "AIS.18",
             "lon": 0.0,
             "lat": 2.0,
             'course' : 0.0,
             'speed' : 0.001},
            {"timestamp": as_timestamp("2018-01-01 02:00"),
             "msgid" : 2,
             "ssvid": "123456789",
             "type": "AIS.1",
             "lon": 0.0,
             "lat": 0.1,
             'course' : 0.0,
             'speed' : 0.001},
            {"timestamp": as_timestamp("2018-01-01 03:00"),
             "msgid" : 3,
             "ssvid": "123456789",
             "type": "AIS.18",
             "lon": 0.0,
             "lat": 1.9,
             'course' : 0.0,
             'speed' : 0.001},
            {"timestamp": as_timestamp("2018-01-01 04:00"),
             "msgid" : 4,
             "ssvid": "123456789",
             "type": "AIS.5",
             "shipname": "Boaty"},
        ]

        segments_in = []
        messages_out, segments_out = self._run_segment(messages_in, segments_in, temp_dir=temp_dir)
        seg_stats = {(seg['seg_id'], seg['message_count'],) for seg in segments_out}

        expected = {('123456789-2018-01-01T00:00:00.000000Z', 2),
                    ('123456789-2018-01-01T01:00:00.000000Z', 2),
                    }
        assert seg_stats == expected

