import sys
import json
from datetime import datetime, timezone

print(sys.path)

from pipe_segment.transform import fragment_implementation  # noqa: E402


def read_json(path, ndx=0):
    i = -1
    ssvid = None
    for line in open(path):
        obj = json.loads(line)
        if ssvid is None or obj["ssvid"] != ssvid:
            i += 1
            ssvid = obj["ssvid"]
        if i == ndx:
            obj["timestamp"] = datetime.fromtimestamp(obj["timestamp"], timezone.utc)
            if "destinations" not in obj:
                obj["destination"] = "over_there"
            yield obj
        if i > ndx:
            break


def read_json_for_one_date(path, ndx=0, date_ndx=0):
    i = -1
    date = None
    for obj in read_json(path, ndx):
        if date is None or obj["timestamp"].date() != date:
            i += 1
            date = obj["timestamp"].date()
        if i == date_ndx:
            yield obj
        if i > date_ndx:
            break


class TestFragmentImp:
    def test_no_crash(self):
        fragger = fragment_implementation.FragmentImplementation()
        for ndx in range(10):
            for dt_ndx in range(10):
                msgs = list(
                    read_json_for_one_date(
                        "tests/data/input.json", ndx=ndx, date_ndx=dt_ndx
                    )
                )
                if not msgs:
                    continue
                for x in fragger.fragment(msgs):
                    pass
