import logging
from datetime import date, timedelta

from apache_beam import FlatMap, PTransform

logger = logging.getLogger(__file__)
logger.setLevel(logging.DEBUG)


class TagWithFragIdAndTimeBin(PTransform):
    def __init__(self, start_date: date, end_date: date, bins_per_days: int):
        self.start_date = start_date
        self.end_date = end_date
        self.bins_per_day = bins_per_days

    def tag_frags(self, x):
        date = self.start_date
        while date <= self.end_date:
            for bin in range(self.bins_per_days):
                yield ((x["frag_id"], str(date), bin), x)
            date += timedelta(days=1)

    def expand(self, xs):
        return xs | FlatMap(self.tag_frags)
