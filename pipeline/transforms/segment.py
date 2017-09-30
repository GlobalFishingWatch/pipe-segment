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
    def _timestamp2datetime(messages):
        # convert the timestamp field in a stream of messages (dicts)
        # from a timestamp to a datetime object
        for msg in messages:
            msg['timestamp'] = timestamp2datetime(msg['timestamp'])
            yield msg

    @staticmethod
    def _datetime2timestamp(messages):
        # convert the timestamp field in a stream of messages (dicts)
        # from a datetime object to an integer value in microseconds

        for msg in messages:
            msg['timestamp'] = datetime2timestamp(msg['timestamp'])
            yield msg

    @staticmethod
    def _gpsdio_segment(messages):
        # for msg in messages:
        #     msg['seg_id'] = '%s-XXX' % msg['mmsi']
        #     yield msg

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
        # Note:  since each of these functions is a generator, the code is not actually executed until
        # the output is iterated in the list() operation at the end

        messages = self._timestamp2datetime(messages)
        messages = self._gpsdio_segment(messages)
        messages = self._datetime2timestamp(messages)

        return (key, list(messages))

    def expand(self, xs):
        return (
            xs | Map(self.segment)
        )
