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

def swap_null(val):
    if val is None:
        val = 'null'
    return val
    
def idents2dict(key, cnt):
    d = {}
    for (id_key, id_val) in key:
        if id_val == 'null':
            id_val = None
        d[id_key] = id_val
    d["count"] = cnt
    return d

def convert_idents(d: dict):
    return [idents2dict(key, cnt) for (key, cnt) in d.items()]
