"""Transform that removes the messages that satellites got with a bad clock."""

import apache_beam as beam
from datetime import datetime, timedelta
import logging
from apache_beam import PTransform

from pipe_segment.tools import datetime_from_timestamp

logger = logging.getLogger(__file__)
logger.setLevel(logging.DEBUG)


class FilterBadSatelliteTimes(PTransform):
    """Filters out the messages that a satellite received in an hour with a bad clock.

    An hour is bad when the offset of the clock of the satellite exceeds ``max_timing_offset_s``.
    The ``bad_hour_padding`` hours on each side of it are discarded too.

    Args:
        satellite_offsets:
            PCollection of the satellite offsets, one per receiver and hour.

        max_timing_offset_s:
            Largest absolute offset of the clock, in seconds, that is accepted.

        bad_hour_padding:
            Number of hours, before and after a bad hour, that are also discarded.

        **kwargs:
            Arguments of PTransform.
    """

    def __init__(
        self, satellite_offsets, max_timing_offset_s, bad_hour_padding, **kwargs
    ):
        """Initializes the transform."""
        super().__init__(**kwargs)
        self.satellite_offsets = satellite_offsets
        self.max_timing_offset_s = max_timing_offset_s
        self.bad_hour_padding = bad_hour_padding

    def exceeds_max_offset(self, x):
        """Returns True if the offset of the clock of a satellite exceeds the maximum.

        Args:
            x:
                A satellite offset, with its ``dt`` in seconds.
        """
        return abs(x["dt"]) > self.max_timing_offset_s

    def make_sat_key(self, receiver, timestamp, offset=0):
        """Returns the key of the hour of a timestamp, for a receiver.

        Args:
            receiver:
                The name of the satellite.

            timestamp:
                Timestamp in seconds.

            offset:
                Hours to add to the hour of the timestamp.

        Returns:
            The key, as ``receiver-year-month-day-hour``.
        """
        dtime = datetime_from_timestamp(timestamp)
        hour = datetime(dtime.year, dtime.month, dtime.day, dtime.hour)
        dt = hour + timedelta(hours=offset)
        return f"{receiver}-{dt.year}-{dt.month}-{dt.day}-{dt.hour}"

    def make_offset_sat_key_set(self, msg):
        """Yields the keys of a bad hour and of the padding hours around it.

        Args:
            msg:
                A satellite offset, with its ``receiver`` and ``hour``.
        """
        for offset in range(-self.bad_hour_padding, self.bad_hour_padding + 1):
            yield self.make_sat_key(msg["receiver"], msg["hour"], offset)

    def not_during_bad_hour(self, msg, bad_hours):
        """Returns True if the message was not received during a bad hour of its receiver.

        Args:
            msg:
                A message, with its ``receiver`` and ``timestamp``.

            bad_hours:
                Dictionary whose keys are the keys of the bad hours.
        """
        return self.make_sat_key(msg["receiver"], msg["timestamp"]) not in bad_hours

    def expand(self, xs):
        """Removes the messages received during bad hours."""
        bad_satellite_hours = (
            self.satellite_offsets
            | beam.Filter(self.exceeds_max_offset)
            | beam.FlatMap(self.make_offset_sat_key_set)
        )
        bad_hours = beam.pvalue.AsDict(bad_satellite_hours | beam.Map(lambda x: (x, x)))

        return xs | "FilterBadSatelliteTimes" >> beam.Filter(
            self.not_during_bad_hour, bad_hours
        )
