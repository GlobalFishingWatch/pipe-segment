import datetime as dt
import itertools as it
from copy import deepcopy

from apache_beam import PTransform
from apache_beam import FlatMap
from apache_beam.pvalue import TaggedOutput

from gpsdio_segment.core import Segmentizer
from gpsdio_segment.core import BadSegment
from gpsdio_segment.core import SegmentState

from pipe_tools.coders import JSONDict

from pipeline.coders import timestamp2datetime
from pipeline.coders import datetime2timestamp
from pipeline.coders import Datetime2TimestampDoFn
from pipeline.stats.stats import MessageStats




class Segment(PTransform):

    OUTPUT_TAG_MESSAGES = 'messages'
    OUTPUT_TAG_SEGMENTS = 'segments'

    DEFAULT_STATS_FIELDS = [('lat', MessageStats.NUMERIC_STATS),
                            ('lon', MessageStats.NUMERIC_STATS),
                            ('timestamp', MessageStats.NUMERIC_STATS),
                            ('shipname', MessageStats.FREQUENCY_STATS),
                            ('imo', MessageStats.FREQUENCY_STATS),
                            ('callsign', MessageStats.FREQUENCY_STATS)]

    def __init__(self, segmenter_params = None,
                 stats_fields=DEFAULT_STATS_FIELDS,
                 **kwargs):
        super(Segment, self).__init__(**kwargs)
        self.segmenter_params = segmenter_params or {}
        self.stats_fields = stats_fields
        # self.stats_numeric_fields = stats_numeric_fields
        # self.stats_frequency_fields = stats_frequency_fields

    @staticmethod
    def _convert_messages_in(msg):
        msg = dict(msg)
        msg['timestamp'] = timestamp2datetime(msg['timestamp'])
        return msg

    @staticmethod
    def _convert_messages_out(msg, seg_id):
        msg = JSONDict(msg)
        msg['timestamp'] = datetime2timestamp(msg['timestamp'])
        msg['seg_id'] = seg_id
        return msg

    @staticmethod
    def stat_output_field_name (field_name, stat_name):
        return '%s_%s' % (field_name, stat_name)

    @classmethod
    def stat_output_field_names (cls, stat_fields):
        for field, stats in stat_fields:
            for stat in stats:
                yield cls.stat_output_field_name(field, stat)

    def _segment_record(self, messages, seg_state):
        stats_numeric_fields = [f for f, stats in self.stats_fields if set(stats) & set (MessageStats.NUMERIC_STATS)]
        stats_frequency_fields = [f for f, stats in self.stats_fields if set(stats) & set (MessageStats.FREQUENCY_STATS)]

        ms = MessageStats(messages, stats_numeric_fields, stats_frequency_fields)

        record = {
            "seg_id": seg_state.id,
            "mmsi": seg_state.mmsi,
            "message_count": seg_state.msg_count
        }

        for field, stats in self.stats_fields:
            stat_values = ms.field_stats(field)
            for stat in stats:
                record[self.stat_output_field_name(field, stat)] = stat_values.get(stat, None)

        return record

    def _gpsdio_segment(self, messages):

        messages = it.imap(self._convert_messages_in, messages)

        for seg in Segmentizer(messages, **self.segmenter_params):
            if isinstance(seg, BadSegment):
                seg_id = "{}-BAD".format(seg.id)
            else:
                seg_id = seg.id

            seg_messages = list(it.imap(self._convert_messages_out, seg, it.repeat(seg_id)))

            seg_record = self._segment_record(seg_messages, seg.state)
            yield TaggedOutput(Segment.OUTPUT_TAG_SEGMENTS, seg_record)

            for msg in seg_messages:
                yield msg

    def segment(self, kv):
        key, messages = kv

        messages = sorted(messages, key=lambda msg: msg['timestamp'])

        for item in self._gpsdio_segment(messages):
            yield item

    def expand(self, xs):
        return (
            xs | FlatMap(self.segment).with_outputs(self.OUTPUT_TAG_SEGMENTS, main=self.OUTPUT_TAG_MESSAGES)
        )
