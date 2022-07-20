import logging

from apache_beam import FlatMap, PTransform

from .util import by_day

logger = logging.getLogger(__file__)
logger.setLevel(logging.DEBUG)


def idents2dict(key, cnt):
    d = dict(key)
    d["count"] = cnt
    return d


def convert_idents(d: dict):
    return [idents2dict(k, v) for (k, v) in d.items()]


class CreateSegments(PTransform):
    def update_msgs(self, items):
        _, frags = items
        frags = sorted(frags, key=lambda x: x["first_msg_timestamp"])
        first_timestamp = frags[0]["first_msg_timestamp"]
        cumulative_msgs = 0
        cumulative_idents = {}
        cumulative_dests = {}

        for _, daily_frags in by_day(frags):
            daily_msgs = 0
            daily_idents = {}
            daily_dests = {}

            for x in daily_frags:

                daily_msgs += x["msg_count"]
                cumulative_msgs += x["msg_count"]

                for ident in x["identities"]:
                    ident = ident.copy()
                    ident_cnt = ident.pop("count")
                    key = tuple(ident.items())
                    daily_idents[key] = daily_idents.get(key, 0) + ident_cnt
                    cumulative_idents[key] = cumulative_idents.get(key, 0) + ident_cnt

                for dest in x["destinations"]:
                    dest = dest.copy()
                    dest_cnt = dest.pop("count")
                    key = tuple(dest.items())
                    daily_dests[key] = daily_dests.get(key, 0) + dest_cnt
                    cumulative_dests[key] = cumulative_dests.get(key, 0) + dest_cnt

            # Only yield last fragment for this seg_id per day.
            seg = x.copy()
            to_delete = {"msg_count", "identities", "destinations"}
            for k in x.keys():
                if k.startswith("first_msg") or k.startswith("last_msg"):
                    to_delete.add(k)
            for k in to_delete:
                del seg[k]

            seg["first_timestamp"] = first_timestamp
            seg["daily_msg_count"] = daily_msgs
            seg["cumulative_msg_count"] = cumulative_msgs
            seg["daily_identities"] = convert_idents(daily_idents)
            seg["cumulative_identities"] = convert_idents(cumulative_idents)
            seg["daily_destinations"] = convert_idents(daily_dests)
            seg["cumulative_destinations"] = convert_idents(cumulative_dests)

            yield seg

    def expand(self, xs):
        return xs | FlatMap(self.update_msgs)
