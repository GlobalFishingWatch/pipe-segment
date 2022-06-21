from datetime import timedelta, date
import logging

from apache_beam import PTransform
from apache_beam import FlatMap

logger = logging.getLogger(__file__)
logger.setLevel(logging.DEBUG)


class TagWithFragIdAndDate(PTransform):
    def __init__(self, start_date: date, end_date: date):
        self.start_date = start_date
        self.end_date = end_date

    def tag_frags(self, x):
        date = self.start_date
        while date <= self.end_date:
            yield ((x["frag_id"], str(date)), x)
            date += timedelta(days=1)

    def expand(self, xs):
        return xs | FlatMap(self.tag_frags)
