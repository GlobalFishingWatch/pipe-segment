import datetime as dt

from apache_beam import PTransform
from apache_beam import Map

from gpsdio_segment.core import Segmentizer
from gpsdio_segment.core import BadSegment

from pipeline.coders import timestamp2datetime
from pipeline.coders import datetime2timestamp


class Segment(PTransform):
    def __init__(self, *args, **kwargs):
        super(Segment, self).__init__(*args, **kwargs)

    @staticmethod
    def _gpsdio_segment(messages):
        for seg in Segmentizer(messages):
            if isinstance(seg, BadSegment):
                seg_id = "{}-BAD".format(seg.id)
            else:
                seg_id = seg.id
            for msg in seg:
                msg['seg_id'] = seg_id
                yield msg

    def segment(self, kv):
        key, messages = kv

        messages = sorted(messages, key=lambda msg: msg['timestamp'])

        return key, list(self._gpsdio_segment(messages))

    def expand(self, xs):
        return (
            xs | Map(self.segment)
        )
