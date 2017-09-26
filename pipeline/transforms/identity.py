import datetime as dt
from apache_beam import PTransform
from apache_beam import Map
from apache_beam import Filter

# Identity transform - passes the message through unchanged
class Identity(PTransform):
    def __init__(self):
        pass

    def expand(self, xs):
        return xs

