import pytest
from datetime import datetime, time

import apache_beam as beam
import newlinejson as nlj
import posixpath

from apache_beam.testing.test_pipeline import TestPipeline as _TestPipeline
from apache_beam.testing.util import open_shards

from json_dict_coder import JSONDictCoder

from pipe_segment.tools import (
    timestamp_from_string, datetime_from_timestamp, timestamp_from_datetime
)
from pipe_segment.transform.fragment import Fragment

# >>> Note that cogroupByKey treats unicode and char values as distinct,
# so tests can sometimes fail unless all ssvid are unicode.


def safe_date(ts):
    if ts is None:
        return None

    return timestamp_from_datetime(datetime.combine(datetime_from_timestamp(ts).date(), time()))


def fill_in_missing_fields(messages):
    messages = messages.copy()
    for msg in messages:
        for k, v in [
            ("course", 90),
            ("callsign", "shippy"),
            ("destination", "generic city name"),
            ("heading", 0.0),
            ("imo", 12345),
            ("length", 100),
            ("lat", None),
            ("lon", None),
            ("msgid", "msgid"),
            ("receiver", "tagblock?"),
            ("receiver_type", "satellite"),
            ("shipname", "shippy-mcshipface"),
            ("shiptype", "raft"),
            ("source", "one of our AIS providers"),
            ("speed", 0),
            ("status", "perfectly fine"),
            ("width", 10),
        ]:
            if k not in msg:
                msg[k] = v
    return messages


def is_subset(superset, subset):
    for super, sub in zip(superset, subset):
        for k, v in sub.items():
            return not super.get(k) != v

    return True


@pytest.mark.filterwarnings("ignore:Using fallback coder:UserWarning")
@pytest.mark.filterwarnings(
    "ignore:The compiler package is deprecated"
    " and removed in Python 3.x.:DeprecationWarning"
)
@pytest.mark.filterwarnings("ignore:open_shards is experimental.:FutureWarning")
class TestTransforms:
    ts = timestamp_from_datetime(datetime(2017, 1, 1, 0, 0, 0))

    @staticmethod
    def _seg_id(ssvid, ts):
        ts = datetime_from_timestamp(ts)
        return "{}-{:%Y-%m-%dT%H:%M:%S.%fZ}".format(ssvid, ts)

    @staticmethod
    def groupby_fn(msg):
        return ((msg["ssvid"], safe_date(msg["timestamp"])), msg)

    @staticmethod
    def convert_timestamp(msg):
        msg["timestamp"] = timestamp_from_datetime(msg["timestamp"])
        return msg

    @staticmethod
    def unconvert_timestamp(msg):
        msg["timestamp"] = datetime_from_timestamp(msg["timestamp"])
        return msg

    def _run_segment(self, messages_in, temp_dir):
        messages_file = posixpath.join(temp_dir, "_run_segment", "messages")
        segments_file = posixpath.join(temp_dir, "_run_segment", "segments")

        messages_in = fill_in_missing_fields(messages_in)

        with _TestPipeline() as p:
            messages = (
                p
                | "CreateMessages" >> beam.Create(messages_in)
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
                messages | "WriteMessages"
                >> beam.io.WriteToText(messages_file, coder=JSONDictCoder())
            )
            segments | "UnconvSegTimes" >> beam.Map(
                self.unconvert_timestamp
            ) | "WriteSegments" >> beam.io.WriteToText(segments_file, coder=JSONDictCoder())

        with open_shards("%s*" % messages_file) as output:
            messages = sorted(
                list(nlj.load(output)), key=lambda m: (m["ssvid"], m["timestamp"])
            )

        with open_shards("%s*" % segments_file) as output:
            segments = list(nlj.load(output))

        assert is_subset(messages, messages_in)

        return messages, segments

    def test_segment_empty(self, tmp_path):
        self._run_segment([], temp_dir=tmp_path)

    def test_segment_single(self, tmp_path):
        messages_in = [{"ssvid": 1, "timestamp": self.ts, "type": "AIS.1"}]
        messages_out, segments_out = self._run_segment(messages_in, temp_dir=tmp_path)

    def test_segment_segments_in(self, tmp_path):
        messages_in = [{"ssvid": "1", "timestamp": self.ts, "type": "AIS.1"}]
        messages_out, segments_out = self._run_segment(messages_in, temp_dir=tmp_path)
        assert messages_out[0]["seg_id"] is None  # No longer assign seg ids to noise segments.

    def test_expected_segments(self, tmp_path):
        messages_in = [
            {
                "timestamp": timestamp_from_string("2017-11-15T11:14:32.000000Z"),
                "ssvid": 257666800,
                "lon": 5.3108466667,
                "lat": 60.40065,
                "speed": 0.0,
                "course": 0.0,
                "type": "AIS.1",
            },
            {
                "timestamp": timestamp_from_string("2017-11-26T11:20:16.000000Z"),
                "ssvid": 257666800,
                "lon": 5.32334,
                "lat": 60.396235,
                "speed": 0.0,
                "course": 0.0,
                "type": "AIS.1",
            },
        ]

        messages_out, segments_out = self._run_segment(messages_in, temp_dir=tmp_path)
        seg_stats = set([(seg["seg_id"], seg["msg_count"]) for seg in segments_out])

        expected = {
            ("257666800-2017-11-15T11:14:32.000000Z-1", 1),
            ("257666800-2017-11-26T11:20:16.000000Z-1", 1),
        }
        assert seg_stats == expected

    def test_message_type(self, tmp_path):
        messages_in = [
            {
                "timestamp": timestamp_from_string("2018-01-01 00:00"),
                "msgid": 0,
                "ssvid": "123456789",
                "type": "AIS.1",
                "lon": 0.0,
                "lat": 0.0,
                "course": 0.0,
                "speed": 0.001,
            },
            {
                "timestamp": timestamp_from_string("2018-01-01 01:00"),
                "msgid": 1,
                "ssvid": "123456789",
                "type": "AIS.18",
                "lon": 0.0,
                "lat": 2.0,
                "course": 0.0,
                "speed": 0.001,
            },
            {
                "timestamp": timestamp_from_string("2018-01-01 02:00"),
                "msgid": 2,
                "ssvid": "123456789",
                "type": "AIS.1",
                "lon": 0.0,
                "lat": 0.1,
                "course": 0.0,
                "speed": 0.001,
            },
            {
                "timestamp": timestamp_from_string("2018-01-01 03:00"),
                "msgid": 3,
                "ssvid": "123456789",
                "type": "AIS.18",
                "lon": 0.0,
                "lat": 1.9,
                "course": 0.0,
                "speed": 0.001,
            },
            {
                "timestamp": timestamp_from_string("2018-01-01 04:00"),
                "msgid": 4,
                "ssvid": "123456789",
                "type": "AIS.5",
                "shipname": "Boaty",
            },
        ]

        messages_out, segments_out = self._run_segment(messages_in, temp_dir=tmp_path)
        seg_stats = {
            (
                seg["seg_id"],
                seg["msg_count"],
            )
            for seg in segments_out
        }

        expected = {
            ("123456789-2018-01-01T00:00:00.000000Z-1", 2),
            ("123456789-2018-01-01T01:00:00.000000Z-1", 2),
        }
        assert seg_stats == expected
