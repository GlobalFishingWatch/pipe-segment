import logging

from apache_beam import PTransform
from apache_beam import FlatMap

logger = logging.getLogger(__file__)
logger.setLevel(logging.DEBUG)


def idents2dict(key, cnt):
    d = dict(key)
    d["count"] = cnt
    return d


class AddCumulativeData(PTransform):
    def update_msgs(self, items):
        _, frags = items
        frags = sorted(frags, key=lambda x: x["timestamp"])
        first_timestamp = frags[0]["first_msg_of_day_timestamp"]
        cumulative_msgs = 0
        cumulative_idents = {}
        for x in frags:
            x = x.copy()
            cumulative_msgs += x["daily_message_count"]
            x["first_timestamp"] = first_timestamp
            x["cumulative_msg_count"] = cumulative_msgs
            daily_idents = x["daily_identities"].copy()
            for ident in daily_idents:
                ident = ident.copy()
                ident_cnt = ident.pop("count")
                key = tuple(ident.items())
                cumulative_idents[key] = cumulative_idents.get(key, 0) + ident_cnt
            x["cumulative_identities"] = [
                idents2dict(k, v) for (k, v) in cumulative_idents.items()
            ]
            yield x

    def expand(self, xs):
        return xs | FlatMap(self.update_msgs)
