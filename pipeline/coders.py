import ujson
from datetime import datetime
from apache_beam.coders import Coder


EPOCH = datetime.utcfromtimestamp(0)


def usec2datetime(ts):
    """Convert an integer timestamp in microseconds to a datetime"""
    return datetime.utcfromtimestamp(ts / 1000000.0)


def datetime2usec(dt):
    """Convert a datetime to an integer timestamp in microseconds"""
    return int((dt - EPOCH).total_seconds() * 1000000.0)


class JSONCoder(Coder):
    """A coder used for reading and writing json"""

    def encode(self, value):
        return ujson.dumps(value)

    def decode(self, value):
        return ujson.loads(value)

    def is_deterministic(self):
        return True
