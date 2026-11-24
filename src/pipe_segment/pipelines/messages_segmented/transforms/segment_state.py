from dataclasses import dataclass
from typing import Any, Dict, Iterable, Optional, Tuple


FLOAT_IDENTITY_FIELDS = ("length", "width")


def identities_to_counter(records: Optional[Iterable[Dict[str, Any]]]) -> Dict[Tuple, int]:
    """Converts repeated identity (or destination) records into a counter.

    The records come from the `segments` table, where every identity field is a STRING,
    while the same fields in the fragments are FLOAT for `length` and `width`. They are
    converted back so that the keys match the ones built from new fragments.

    Args:
        records: records as read from BigQuery. Each one has a `count` and the identity fields.

    Returns:
        A dictionary from a tuple of (field, value) pairs to the count, in the same order
        as the records.
    """
    counter: Dict[Tuple, int] = {}
    for record in records or []:
        record = dict(record)
        count = record.pop("count")
        for name in FLOAT_IDENTITY_FIELDS:
            if record.get(name) is not None:
                record[name] = float(record[name])
        counter[tuple(record.items())] = count
    return counter


@dataclass(frozen=True)
class SegmentState:
    """State of a segment at the end of a day.

    It is what the next day needs from the previous one: the fragment the segment ended
    with (to score the match against the new fragments) and the cumulative values (to add
    the new days on top).
    """

    seg_id: str
    frag_id: str
    ssvid: str
    first_timestamp: float
    cumulative_msg_count: int
    cumulative_identities: Dict[Tuple, int]
    cumulative_destinations: Dict[Tuple, int]
    last_msg_timestamp: float
    last_msg_lat: float
    last_msg_lon: float
    last_msg_course: Optional[float]
    last_msg_speed: float

    @classmethod
    def from_row(cls, row: Dict[str, Any]) -> "SegmentState":
        """Builds the state from a row of the prior segments query."""
        return cls(
            seg_id=row["seg_id"],
            frag_id=row["frag_id"],
            ssvid=row["ssvid"],
            first_timestamp=row["first_timestamp"],
            cumulative_msg_count=row["cumulative_msg_count"],
            cumulative_identities=identities_to_counter(row["cumulative_identities"]),
            cumulative_destinations=identities_to_counter(row["cumulative_destinations"]),
            last_msg_timestamp=row["last_msg_timestamp"],
            last_msg_lat=row["last_msg_lat"],
            last_msg_lon=row["last_msg_lon"],
            last_msg_course=row["last_msg_course"],
            last_msg_speed=row["last_msg_speed"],
        )

    def as_last_fragment(self) -> Dict[str, Any]:
        """Returns the part of the last fragment that is needed to score a match."""
        return {
            "frag_id": self.frag_id,
            "ssvid": self.ssvid,
            "last_msg_timestamp": self.last_msg_timestamp,
            "last_msg_lat": self.last_msg_lat,
            "last_msg_lon": self.last_msg_lon,
            "last_msg_course": self.last_msg_course,
            "last_msg_speed": self.last_msg_speed,
        }
