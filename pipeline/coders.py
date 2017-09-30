import ujson
from apache_beam.coders import Coder


class JSONCoder(Coder):
    """A coder used for reading and writing json"""

    def encode(self, value):
        return ujson.dumps(value)

    def decode(self, value):
        return ujson.loads(value)

    def is_deterministic(self):
        return True
