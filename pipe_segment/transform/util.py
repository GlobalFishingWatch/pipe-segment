from pipe_segment.tools import datetimeFromTimestamp


def by_day(items, key="timestamp"):
    items = sorted(items, key=lambda x: x[key])
    day = datetimeFromTimestamp(items[0][key]).date()

    current = []
    for x in items:
        new_day = datetimeFromTimestamp(x[key]).date()
        if new_day != day:
            yield day, current

            current = []
            day = new_day
        current.append(x)

    yield day, current
