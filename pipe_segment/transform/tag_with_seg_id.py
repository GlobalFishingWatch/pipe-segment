import logging

from apache_beam import PTransform
from apache_beam import FlatMap

logger = logging.getLogger(__file__)
logger.setLevel(logging.DEBUG)


class TagWithSegId(PTransform):
    def tag_msgs(self, keyed_items):
        key, items = keyed_items
        objs = items["target"]
        frag_map = {x["frag_id"]: x["seg_id"] for x in items["fragmap"]}
        for x in objs:
            x = x.copy()
            x["seg_id"] = frag_map.get(x["frag_id"], None)
            yield x

    def expand(self, xs):
        return xs | FlatMap(self.tag_msgs)
