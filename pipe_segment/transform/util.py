from datetime import date
from typing import Iterable, Generator, Tuple, List

from pipe_segment.tools import datetime_from_timestamp


def by_day(
    items: Iterable[dict], key: str = "timestamp"
) -> Generator[Tuple[date, List[dict]], None, None]:
    """Yield items grouped by date with the groups in date order"""
    items = sorted(items, key=lambda x: x[key])
    current: List[dict] = []
    day = datetime_from_timestamp(items[0][key]).date()
    for x in items:
        new_day = datetime_from_timestamp(x[key]).date()
        if new_day != day:
            assert len(current) > 0
            yield day, current
            current = []
            day = new_day
        current.append(x)
    assert len(current) > 0
    yield day, current
