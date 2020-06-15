import bisect
import datetime as dt
import itertools as it
import logging
import six

import apache_beam as beam
from apache_beam import PTransform
from apache_beam import Map

logger = logging.getLogger(__file__)
logger.setLevel(logging.DEBUG)

class AddNoiseFlag(PTransform):

    def __init__(self, min_secondary_track_count):
        self.min_secondary_track_count = min_secondary_track_count

    def is_noise(self, v):
        return not (v['index'] == 0 or v['count'] >= self.min_secondary_track_count)

    def add_noise_flag(self, v):
        v['is_noise'] = self.is_noise(v)
        return v

    def expand(self, xs):
        return (
            xs | Map(self.add_noise_flag)
        )
