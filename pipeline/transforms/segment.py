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


class Segment(PTransform):

    OUTPUT_TAG_MESSAGES = 'messages'
    OUTPUT_TAG_SEGMENTS = 'segments'

    def __init__(self, segmenter_params = None, **kwargs):
        super(Segment, self).__init__(**kwargs)
        self.segmenter_params = segmenter_params or {}

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

    def _gpsdio_segment(self, messages):
        for seg in Segmentizer(messages, **self.segmenter_params):
            if isinstance(seg, BadSegment):
                seg_id = "{}-BAD".format(seg.id)
            else:
                seg_id = seg.id

            yield TaggedOutput(Segment.OUTPUT_TAG_SEGMENTS, self._convert_seg_state(seg.state))

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
