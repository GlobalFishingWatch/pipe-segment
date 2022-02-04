import datetime
import ujson
import apache_beam as beam
import six


EPOCH = datetime.utcfromtimestamp(0)


def timestampFromDatetime(dt):
    """Convert a datetime to a unix timestamp"""
    return (dt - EPOCH).total_seconds()


def datetimeFromTimestamp(ts):
    """Convert a unix timestamp to a datetime"""
    return datetime.fromtimestamp(ts, tz=pytz.UTC)


class JSONDictCoder(beam.coders.Coder):
    """A coder used for reading and writing json"""

    def encode(self, value):
        return six.ensure_binary(ujson.dumps(value))

    def decode(self, value):
        return ujson.loads(six.ensure_str(value))

    def is_deterministic(self):
        return True
