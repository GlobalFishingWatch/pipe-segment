import logging
from datetime import date

from apache_beam import FlatMap, PTransform

logger = logging.getLogger(__file__)
logger.setLevel(logging.DEBUG)


class TagWithFragIdAndTimeBin(PTransform):
    def __init__(self, start_date: date, end_date: date, bins_per_day: int):
        self.start_date = start_date
        self.end_date = end_date
        self.bins_per_day = bins_per_day

    def tag_frags(self, x):
        if self.start_date <= x["date"] <= self.end_date:
            for sub_bin in range(self.bins_per_day):
                yield ((x["frag_id"], x["date"], sub_bin), x)

    def expand(self, xs):
        return xs | FlatMap(self.tag_frags)
