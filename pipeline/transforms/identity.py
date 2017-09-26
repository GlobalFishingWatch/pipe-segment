import datetime as dt
from apache_beam import PTransform
from apache_beam import Map
from apache_beam import Filter

# Identity transform - passes the message through unchanged
class Identity(PTransform):

    def expand(self, xs):
        return xs

