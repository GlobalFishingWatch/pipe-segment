import pytest
from datetime import datetime, time
import pytz
import posixpath as pp
import newlinejson as nlj

import apache_beam as beam

from apache_beam.testing.test_pipeline import TestPipeline as _TestPipeline

# rename the class to prevent py.test from trying to collect TestPipeline as a unit test class

from apache_beam.testing.util import BeamAssertException
from apache_beam.testing.util import open_shards

from pipe_segment.tools import as_timestamp
from pipe_segment.tools import timestampFromDatetime
from pipe_segment.tools import datetimeFromTimestamp
from pipe_segment.transform.fragment import Fragment

from json_dict_coder import JSONDictCoder


# >>> Note that cogroupByKey treats unicode and char values as distinct,
# so tests can sometimes fail unless all ssvid are unicode.

# for use with apache_beam.testing.util.assert_that
# for pcollections that contain dicts
#
# for example
# assert_that(pcoll, contains(expected))


def safe_date(ts):
    if ts is None:
        return None
    return timestampFromDatetime(
        datetime.combine(datetimeFromTimestamp(ts).date(), time()).replace(
            tzinfo=pytz.UTC
        )
    )


def contains(subset):
    subset = list(subset)

    def _contains(superset):
        sorted_subset = sorted(subset)
        sorted_superset = sorted(list(superset))
        for sub, super in zip(sorted_subset, sorted_superset):
            for k, v in sub.items():
                if super.get(k) != v:
                    raise BeamAssertException(
                        "Failed assert: %s does not contain %s\n"
                        'mismatch in key "%s"  %s != %s'
                        % (super, sub, k, super.get(k), v)
                    )

    return _contains


def list_contains(superset, subset):
    for super, sub in zip(superset, subset):
        for k, v in sub.items():
            if super.get(k) != v:
                raise BeamAssertException(
                    "Failed assert: %s does not contain %s\n"
                    'mismatch in key "%s"  %s != %s' % (super, sub, k, super.get(k), v)
                )
    return True


@pytest.mark.filterwarnings("ignore:Using fallback coder:UserWarning")
@pytest.mark.filterwarnings(
    "ignore:The compiler package is deprecated and removed in Python 3.x.:DeprecationWarning"
)
@pytest.mark.filterwarnings("ignore:open_shards is experimental.:FutureWarning")
class TestTransforms:
    ts = timestampFromDatetime(datetime(2017, 1, 1, 0, 0, 0, tzinfo=pytz.UTC))

    @staticmethod
    def _seg_id(ssvid, ts):
        ts = datetimeFromTimestamp(ts)
        return "{}-{:%Y-%m-%dT%H:%M:%S.%fZ}".format(ssvid, ts)

    @staticmethod
    def groupby_fn(msg):
        return ((msg["ssvid"], safe_date(msg["timestamp"])), msg)

    @staticmethod
    def convert_timestamp(msg):
        msg["timestamp"] = timestampFromDatetime(
            msg["timestamp"].replace(tzinfo=pytz.UTC)
        )
        return msg

    @staticmethod
    def unconvert_timestamp(msg):
        msg["timestamp"] = datetimeFromTimestamp(msg["timestamp"])
        return msg

    def _run_segment(self, messages_in, temp_dir):
        messages_file = pp.join(temp_dir, "_run_segment", "messages")
        segments_file = pp.join(temp_dir, "_run_segment", "segments")

        print(messages_in)

        with _TestPipeline() as p:
            messages = (
                p
                | "CreateMessages" >> beam.Create(messages_in)
                # | "ConvertTimestamps" >> beam.Map(self.convert_timestamp)
                | "AddKeyMessages" >> beam.Map(self.groupby_fn)
                | "GroupByKey" >> beam.GroupByKey()
            )

            segmentizer = Fragment()

            def add_seg_id(msg):
                msg["seg_id"] = msg["frag_id"]
                return msg

            segmented = messages | "Segment" >> segmentizer
            messages = segmented["messages"] | beam.Map(add_seg_id)
            segments = segmented[
                segmentizer.OUTPUT_TAG_FRAGMENTS
            ] | "AddSegIdToFrags" >> beam.Map(add_seg_id)

            (
                messages
                |  # "UnconvMsgTimes" >> beam.Map(
                # self.unconvert_timestamp
                # ) |
                "WriteMessages"
                >> beam.io.WriteToText(messages_file, coder=JSONDictCoder())
            )
            segments | "UnconvSegTimes" >> beam.Map(
                self.unconvert_timestamp
            ) | "WriteSegments" >> beam.io.WriteToText(
                segments_file, coder=JSONDictCoder()
            )

            p.run()

            with open_shards("%s*" % messages_file) as output:
                messages = sorted(
                    list(nlj.load(output)), key=lambda m: (m["ssvid"], m["timestamp"])
                )
            with open_shards("%s*" % segments_file) as output:
                segments = list(nlj.load(output))

            assert list_contains(messages, messages_in)
            return messages, segments

    def test_segment_empty(self, temp_dir):
        self._run_segment([], temp_dir=temp_dir)

    def test_segment_single(self, temp_dir):
        messages_in = [{"ssvid": 1, "timestamp": self.ts, "type": "AIS.1"}]
        messages_out, segments_out = self._run_segment(messages_in, temp_dir=temp_dir)

    def test_segment_segments_in(self, temp_dir):
        messages_in = [{"ssvid": "1", "timestamp": self.ts, "type": "AIS.1"}]
        messages_out, segments_out = self._run_segment(messages_in, temp_dir=temp_dir)
        assert (
            messages_out[0]["seg_id"] == None
        )  # No longer assign seg ids to noise segments

    def test_expected_segments(self, temp_dir):
        messages_in = [
            {
                "timestamp": as_timestamp("2017-11-15T11:14:32.000000Z"),
                "ssvid": 257666800,
                "lon": 5.3108466667,
                "lat": 60.40065,
                "speed": 0.0,
                "course": 0.0,
                "type": "AIS.1",
            },
            {
                "timestamp": as_timestamp("2017-11-26T11:20:16.000000Z"),
                "ssvid": 257666800,
                "lon": 5.32334,
                "lat": 60.396235,
                "speed": 0.0,
                "course": 0.0,
                "type": "AIS.1",
            },
        ]

        messages_out, segments_out = self._run_segment(messages_in, temp_dir=temp_dir)
        seg_stats = set(
            [(seg["seg_id"], seg["daily_message_count"]) for seg in segments_out]
        )
        print(segments_out)
        expected = {
            ("257666800-2017-11-15T11:14:32.000000Z", 1),
            ("257666800-2017-11-26T11:20:16.000000Z", 1),
        }
        assert seg_stats == expected

    def test_message_type(self, temp_dir):
        messages_in = [
            {
                "timestamp": as_timestamp("2018-01-01 00:00"),
                "msgid": 0,
                "ssvid": "123456789",
                "type": "AIS.1",
                "lon": 0.0,
                "lat": 0.0,
                "course": 0.0,
                "speed": 0.001,
            },
            {
                "timestamp": as_timestamp("2018-01-01 01:00"),
                "msgid": 1,
                "ssvid": "123456789",
                "type": "AIS.18",
                "lon": 0.0,
                "lat": 2.0,
                "course": 0.0,
                "speed": 0.001,
            },
            {
                "timestamp": as_timestamp("2018-01-01 02:00"),
                "msgid": 2,
                "ssvid": "123456789",
                "type": "AIS.1",
                "lon": 0.0,
                "lat": 0.1,
                "course": 0.0,
                "speed": 0.001,
            },
            {
                "timestamp": as_timestamp("2018-01-01 03:00"),
                "msgid": 3,
                "ssvid": "123456789",
                "type": "AIS.18",
                "lon": 0.0,
                "lat": 1.9,
                "course": 0.0,
                "speed": 0.001,
            },
            {
                "timestamp": as_timestamp("2018-01-01 04:00"),
                "msgid": 4,
                "ssvid": "123456789",
                "type": "AIS.5",
                "shipname": "Boaty",
            },
        ]

        messages_out, segments_out = self._run_segment(messages_in, temp_dir=temp_dir)
        seg_stats = {
            (
                seg["seg_id"],
                seg["daily_message_count"],
            )
            for seg in segments_out
        }

        expected = {
            ("123456789-2018-01-01T00:00:00.000000Z", 2),
            ("123456789-2018-01-01T01:00:00.000000Z", 2),
        }
        assert seg_stats == expected
