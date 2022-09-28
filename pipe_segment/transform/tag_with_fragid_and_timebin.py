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
        frag_id = x["frag_id"]
        date_str = frag_id.split("T")[0].split("-", 1)[1]
        assert len(date_str) == 10
        start_date_str = f"{self.start_date:%Y-%m-%d}"
        end_date_str = f"{self.end_date:%Y-%m-%d}"
        if start_date_str <= date_str <= end_date_str:
            for bin in range(self.bins_per_day):
                yield ((x["frag_id"], str(date), bin), x)

    def expand(self, xs):
        return xs | FlatMap(self.tag_frags)
