import ujson
from datetime import datetime
from apache_beam.coders import Coder


EPOCH = datetime.utcfromtimestamp(0)


def timestamp2datetime(ts):
    """Convert a timestamp in seconds with microsecond precision to a datetime"""
    return datetime.utcfromtimestamp(ts)


def datetime2timestamp(dt):
    """Convert a datetime to a timestamp in seconds with microsecond precision"""
    return (dt - EPOCH).total_seconds()


class JSONCoder(Coder):
    """A coder used for reading and writing json"""

    def encode(self, value):
        return ujson.dumps(value)

    def decode(self, value):
        return ujson.loads(value)

    def is_deterministic(self):
        return True
