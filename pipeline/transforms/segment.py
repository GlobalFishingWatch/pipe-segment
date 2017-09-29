import datetime as dt
from apache_beam import PTransform
from apache_beam import Map
from apache_beam import Filter

from gpsdio_segment.core import Segmentizer

class Segment(PTransform):
    def __init__(self):
        pass

    def expand(self, xs):
        return (
            xs
        )
