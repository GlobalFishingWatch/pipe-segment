import ujson
from datetime import datetime

import apache_beam as beam
from apache_beam.coders import Coder
from apache_beam.transforms import window

EPOCH = datetime.utcfromtimestamp(0)


def timestamp2datetime(ts):
    """Convert a timestamp in seconds with microsecond precision to a datetime"""
    return datetime.utcfromtimestamp(ts)


def datetime2timestamp(dt):
    """Convert a datetime to a timestamp in seconds with microsecond precision"""
    return (dt - EPOCH).total_seconds()


class AddTimestampDoFn(beam.DoFn):

  def process(self, msg):
    # Wrap and emit the current entry and new timestamp in a
    # TimestampedValue.
    yield window.TimestampedValue(msg, msg['timestamp'])


class Timestamp2DatetimeDoFn(beam.DoFn):
    def process(self, msg):
        # convert the timestamp field from a unix timestamp to a datetime
        msg = dict(msg)
        msg['timestamp'] = timestamp2datetime(msg['timestamp'])
        yield msg


class Datetime2TimestampDoFn(beam.DoFn):
    def process(self, msg):
        # convert the timestamp field from a datetime to a unix timestamp
        msg = dict(msg)
        msg['timestamp'] = datetime2timestamp(msg['timestamp'])
        yield msg


class JSONCoder(Coder):
    """A coder used for reading and writing json"""

    def encode(self, value):
        return ujson.dumps(value)

    def decode(self, value):
        return ujson.loads(value)

    def is_deterministic(self):
        return True
