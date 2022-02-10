import apache_beam as beam
from datetime import datetime, timedelta
import logging
from apache_beam import PTransform

from ..tools import datetimeFromTimestamp

logger = logging.getLogger(__file__)
logger.setLevel(logging.DEBUG)


class FilterBadSatelliteTimes(PTransform):
    def __init__(
        self, satellite_offsets, max_timing_offset_s, bad_hour_padding, **kwargs
    ):
        super().__init__(**kwargs)
        self.satellite_offsets = satellite_offsets
        self.max_timing_offset_s = max_timing_offset_s
        self.bad_hour_padding = bad_hour_padding

    def exceeds_max_offset(self, x):
        return abs(x["dt"]) > self.max_timing_offset_s

    def make_sat_key(self, receiver, timestamp, offset=0):
        dtime = datetimeFromTimestamp(timestamp)
        hour = datetime(dtime.year, dtime.month, dtime.day, dtime.hour)
        dt = hour + timedelta(hours=offset)
        return f"{receiver}-{dt.year}-{dt.month}-{dt.day}-{dt.hour}"

    def make_offset_sat_key_set(self, msg):
        for offset in range(-self.bad_hour_padding, self.bad_hour_padding + 1):
            yield self.make_sat_key(msg["receiver"], msg["hour"], offset)

    def not_during_bad_hour(self, msg, bad_hours):
        return self.make_sat_key(msg["receiver"], msg["timestamp"]) not in bad_hours

    def expand(self, xs):
        bad_satellite_hours = (
            self.satellite_offsets
            | beam.Filter(self.exceeds_max_offset)
            | beam.FlatMap(self.make_offset_sat_key_set)
        )
        bad_hours = beam.pvalue.AsDict(bad_satellite_hours | beam.Map(lambda x: (x, x)))

        return xs | "FilterBadSatelliteTimes" >> beam.Filter(
            self.not_during_bad_hour, bad_hours
        )
