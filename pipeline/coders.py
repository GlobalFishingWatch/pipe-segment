import ujson
from datetime import datetime

import apache_beam as beam
from apache_beam.coders import Coder
from apache_beam.transforms import window

EPOCH = datetime.utcfromtimestamp(0)
TIMESTAMP_FORMAT = "%Y-%m-%d %H:%M:%S.%f %Z"

def timestamp2datetime(ts):
    """Convert a beam Timestamp  to a datetime"""
    return datetime.utcfromtimestamp(ts)

def datetime2timestamp(dt):
    """Convert a datetime to a beam Timestamp """
    return (dt - EPOCH).total_seconds()

def str2datetime(s):
    """convert a string representation of a timestamp to a datetime"""
    return  datetime.strptime(s, TIMESTAMP_FORMAT)

def datetime2str(dt):
    """convert a datetime to a string"""
    return dt.strftime(TIMESTAMP_FORMAT)

def str2timestamp(s):
    """convert a string representation of to a Timestamp"""
    return datetime2timestamp(datetime.strptime(s, TIMESTAMP_FORMAT))



class Timestamp2DatetimeDoFn(beam.DoFn):
    def process(self, msg):
        # convert the timestamp field from a unix timestamp to a datetime
        msg = dict(msg)
        print msg
        msg['timestamp'] = timestamp2datetime(msg['timestamp'])
        yield msg


class Datetime2TimestampDoFn(beam.DoFn):
    def process(self, msg):
        # convert the timestamp field from a datetime to a unix timestamp
        msg = dict(msg)
        msg['timestamp'] = datetime2timestamp(msg['timestamp'])
        yield msg


class AddTimestampDoFn(beam.DoFn):
    """Use this to extract a timestamp from a message.  Expects a dict with a
    field named "timestamp" containing a unix timestamp"""
    def process(self, msg):
        # Wrap and emit the current entry and new timestamp in a
        # TimestampedValue.
        yield window.TimestampedValue(msg, msg['timestamp'])


class JSONCoder(Coder):
    """A coder used for reading and writing json"""

    def encode(self, value):
        return ujson.dumps(value)

    def decode(self, value):
        return ujson.loads(value)

    def is_deterministic(self):
        return True
