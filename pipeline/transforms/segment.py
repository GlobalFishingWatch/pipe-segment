import datetime as dt
from copy import deepcopy

from apache_beam import PTransform
from apache_beam import FlatMap
from apache_beam.pvalue import TaggedOutput

from gpsdio_segment.core import Segmentizer
from gpsdio_segment.core import BadSegment
from gpsdio_segment.core import SegmentState

from pipeline.coders import timestamp2datetime
from pipeline.coders import datetime2timestamp
from pipeline.coders import Datetime2TimestampDoFn


class MMSISegmentizer(object):
    """
    Wrapper class for `gpsdio_segment.core.Segmentizer()`.  Manages
    serialization and lifetimes of segment states that are preserved between
    consecutive processing runs.
    """

    def __init__(self, seg_states, timestamp):
        self._prev_seg_states = list(map(self._state_from_dict, seg_states))
        self._new_seg_states = []
        self.timestamp = timestamp

    def process(self, messages):
        for seg in Segmentizer.from_seg_states(self._prev_seg_states, messages):
            if isinstance(seg, BadSegment):
                seg_id = "{}-BAD".format(seg.id)
            else:
                self._new_seg_states.append(seg.state)
                seg_id = seg.id
            for msg in seg:
                msg['seg_id'] = seg_id
                yield msg

    def seg_states(self):
        for st in self._new_seg_states:
            if self.keep_seg_state(st):
                yield self._state_to_dict(st)

    @classmethod
    def _state_from_dict(cls, st):
        st = SegmentState.from_dict(st)
        st.msgs = list(map(cls._load_timestamp, st.msgs))
        return st

    @classmethod
    def _state_to_dict(cls, st):
        st.msgs = list(map(cls._dump_timestamp, st.msgs))
        st = SegmentState.to_dict(st)
        return st

    @classmethod
    def _load_timestamp(cls, msg):
        dt = msg.get('timestamp', tools.AbsentKey)
        if dt is not tools.AbsentKey:
            msg['timestamp'] = tools.str2datetime(dt)
        return msg

    @classmethod
    def _dump_timestamp(cls, msg):
        dt = msg.get('timestamp', tools.AbsentKey)
        if dt is not tools.AbsentKey:
            msg['timestamp'] = tools.datetime2str(dt)
        return msg


class Segment(PTransform):

    OUTPUT_TAG_MESSAGES = 'messages'
    OUTPUT_TAG_SEGMENTS = 'segments'

    def __init__(self, *args, **kwargs):
        super(Segment, self).__init__(*args, **kwargs)

    @classmethod
    def _datetime2timestamp(cls, msg):
        msg = deepcopy(msg)
        msg['timestamp'] = datetime2timestamp(msg['timestamp'])
        return msg

    @classmethod
    def _convert_seg_state(cls, st):
        st.msgs = map(cls._datetime2timestamp, st.msgs)
        res = {"seg_id": st.id,
               "mmsi": st.mmsi,
               "message_count": st.msg_count
               }
        return res

    @classmethod
    def _gpsdio_segment(cls, messages):
        for seg in Segmentizer(messages):
            if isinstance(seg, BadSegment):
                seg_id = "{}-BAD".format(seg.id)
            else:
                seg_id = seg.id

            yield TaggedOutput(Segment.OUTPUT_TAG_SEGMENTS, cls._convert_seg_state(seg.state))

            for msg in seg:
                msg['seg_id'] = seg_id
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
