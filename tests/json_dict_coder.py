import datetime
import ujson
import apache_beam as beam
import six


class JSONDictCoder(beam.coders.Coder):
    """A coder used for reading and writing json"""

    def encode(self, value):
        if isinstance(value, dict):
            value = value.copy()
            for k, v in value.items():
                if isinstance(v, datetime.datetime):
                    value[k] = v.timestamp()
        return six.ensure_binary(ujson.dumps(value))

    def decode(self, value):
        return ujson.loads(six.ensure_str(value))

    def is_deterministic(self):
        return True
