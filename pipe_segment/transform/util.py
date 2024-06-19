from pipe_segment.tools import datetime_from_timestamp


def by_day(items, key="timestamp"):
    items = sorted(items, key=lambda x: x[key])
    current = []
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
