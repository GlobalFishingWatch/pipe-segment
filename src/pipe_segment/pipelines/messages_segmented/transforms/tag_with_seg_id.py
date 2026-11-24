import logging

from apache_beam import FlatMap, PTransform

logger = logging.getLogger(__file__)
logger.setLevel(logging.DEBUG)


class TagWithSegId(PTransform):
    def tag_msgs(self, keyed_items):
        key, items = keyed_items

        for x in items["segmap"]:
            if x["seg_id"] is None:
                raise ValueError("Received seg_id=None at item {}".format(x))

        seg_map = {x["frag_id"]: x["seg_id"] for x in items["segmap"]}

        objs = items["target"]
        for x in objs:
            x = x.copy()
            x["seg_id"] = seg_map.get(x["frag_id"], None)
            yield x

    def expand(self, xs):
        return xs | FlatMap(self.tag_msgs)
