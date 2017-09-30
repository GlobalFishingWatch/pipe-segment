import datetime as dt
from apache_beam import PTransform
from apache_beam import Map
from apache_beam import Filter

from gpsdio_segment.core import Segmentizer

class Segment(PTransform):
    def __init__(self):
        pass

    def segment(self, kv):
        key, messages = kv

        # TODO: segment messages here

        return (key, messages)

    def expand(self, xs):
        return (
            xs | Map(self.segment)
        )

