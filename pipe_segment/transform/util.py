from ..tools import datetimeFromTimestamp


def by_day(items, key="timestamp"):
    items = sorted(items, key=lambda x: x[key])
    current = []
    day = datetimeFromTimestamp(items[0][key]).date()
    for x in items:
        new_day = datetimeFromTimestamp(x[key]).date()
        if new_day != day:
            assert len(current) > 0
            yield day, current
            current = []
            day = new_day
        current.append(x)
    assert len(current) > 0
    yield day, current
