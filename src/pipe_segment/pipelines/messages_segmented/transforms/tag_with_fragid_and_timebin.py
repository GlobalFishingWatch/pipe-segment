"""Transform that keys the segment map by the time bins of its fragments."""

import logging
from datetime import date

from apache_beam import FlatMap, PTransform

logger = logging.getLogger(__file__)
logger.setLevel(logging.DEBUG)


class TagWithFragIdAndTimeBin(PTransform):
    """Keys each row of the segment map once per time bin of the day of its fragment.

    Rows of days out of the date range are dropped.

    Args:
        start_date:
            First day to keep.

        end_date:
            Last day to keep.

        bins_per_day:
            Number of time bins that a day is split in.
    """

    def __init__(self, start_date: date, end_date: date, bins_per_day: int):
        """Initializes the transform."""
        self.start_date = start_date
        self.end_date = end_date
        self.bins_per_day = bins_per_day

    def tag_frags(self, x):
        """Yields the row once per time bin, keyed by ssvid, fragment, day and bin.

        Args:
            x:
                A row of the segment map, with its ``ssvid``, ``frag_id`` and ``date``.
        """
        if self.start_date <= x["date"] <= self.end_date:
            for sub_bin in range(self.bins_per_day):
                yield ((x["ssvid"], x["frag_id"], str(x["date"]), sub_bin), x)

    def expand(self, xs):
        """Keys the rows of the segment map by time bin."""
        return xs | FlatMap(self.tag_frags)
