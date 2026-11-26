"""Transform that tags messages and fragments with the id of their segment."""

import logging

from apache_beam import FlatMap, PTransform

logger = logging.getLogger(__file__)
logger.setLevel(logging.DEBUG)


class TagWithSegId(PTransform):
    """Tags each item with the seg_id of its fragment, taken from the segment map.

    Its input is the result of a CoGroupByKey, with the rows of the segment map under ``segmap``
    and the items to tag under ``target``.
    """

    def tag_msgs(self, keyed_items):
        """Yields a copy of each item to tag, with its seg_id.

        Args:
            keyed_items:
                Tuple of the key and the grouped rows of the segment map and items to tag.

        Yields:
            The items, with the seg_id of their fragment, or None if the map does not have it.

        Raises:
            ValueError: If a row of the segment map has no seg_id.
        """
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
        """Tags the items grouped with the rows of the segment map."""
        return xs | FlatMap(self.tag_msgs)
