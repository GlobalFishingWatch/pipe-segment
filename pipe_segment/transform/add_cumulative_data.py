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


class AddCumulativeData(PTransform):
    def update_msgs(self, items):
        _, frags = items
        frags = sorted(frags, key=lambda x: x["timestamp"])
        first_timestamp = frags[0]["first_msg_of_day_timestamp"]
        cumulative_msgs = 0
        cumulative_idents = {}
        cumulative_dests = {}

        for _, daily_frags in by_day(frags):
            daily_msgs = 0
            daily_idents = {}
            daily_dests = {}

            for x in daily_frags:
                x = x.copy()

                daily_msgs += x["daily_message_count"]
                cumulative_msgs += x["daily_message_count"]
                x["first_timestamp"] = first_timestamp
                x["daily_message_count"] = daily_msgs
                x["cumulative_msg_count"] = cumulative_msgs
                for ident in x["daily_identities"]:
                    ident = ident.copy()
                    ident_cnt = ident.pop("count")
                    key = tuple(ident.items())
                    daily_idents[key] = daily_idents.get(key, 0) + ident_cnt
                    cumulative_idents[key] = cumulative_idents.get(key, 0) + ident_cnt
                x["daily_identities"] = convert_idents(daily_idents)
                x["cumulative_identities"] = convert_idents(cumulative_idents)

                for dest in x["daily_destinations"]:
                    dest = dest.copy()
                    dest_cnt = dest.pop("count")
                    key = tuple(dest.items())
                    daily_dests[key] = daily_dests.get(key, 0) + dest_cnt
                    cumulative_dests[key] = cumulative_dests.get(key, 0) + dest_cnt
                x["daily_destinations"] = convert_idents(daily_dests)
                x["cumulative_destinations"] = convert_idents(cumulative_dests)
            # Only yield last fragment for this seg_id per day.
            yield x

    def expand(self, xs):
        return xs | FlatMap(self.update_msgs)
