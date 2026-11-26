import logging
import math
import pytz

from apache_beam import FlatMap, PTransform
from apache_beam.pvalue import TaggedOutput

from pipe_segment.tools import datetime_from_timestamp, timestamp_from_datetime
from .fragment_implementation import FragmentImplementation

logger = logging.getLogger(__file__)
logger.setLevel(logging.DEBUG)


def none_to_inf(x):
    return math.inf if (x is None) else x


def none_to_blank(x):
    return "" if (x is None) else x


class Fragment(PTransform):

    OUTPUT_TAG_FRAGMENTS = FragmentImplementation.OUTPUT_TAG_FRAGMENTS
    OUTPUT_TAG_MESSAGES = FragmentImplementation.OUTPUT_TAG_MESSAGES

    def __init__(
        self,
        fragmenter_params=None,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self._fragmenter = FragmentImplementation(fragmenter_params)

    @staticmethod
    def _convert_message_in(msg):
        msg = msg.copy()
        msg["raw_timestamp"] = msg["timestamp"]
        # TODO remove pytz once gspdio-segment is updated.
        msg["timestamp"] = datetime_from_timestamp(msg["raw_timestamp"], tzinfo=pytz.UTC)
        return msg

    @staticmethod
    def _convert_message_out(msg):
        msg = msg.copy()
        msg["timestamp"] = msg.pop("raw_timestamp")
        return msg

    @staticmethod
    def _convert_fragment_out(frag):
        frag = dict(frag.items())
        for k in [
            "timestamp",
            "first_msg_timestamp",
            "last_msg_timestamp",
        ]:
            if k not in frag:
                raise KeyError(f"Required key {k} is not found in the fragment {frag}")
            if not frag[k] is None:
                frag[k] = timestamp_from_datetime(frag[k])
        return frag

    def fragment(self, item):
        _, messages = item
        messages = [self._convert_message_in(x) for x in messages]
        messages.sort(
            key=lambda x: (
                x["timestamp"],
                x["msgid"],
                none_to_inf(x["lon"]),
                none_to_inf(x["lat"]),
                none_to_inf(x["speed"]),
                none_to_inf(x["course"]),
                none_to_inf(x.get("heading")),
                none_to_blank(x.get("destination")),
                none_to_inf(x.get("length")),
                none_to_inf(x.get("width")),
                none_to_blank(x["shiptype"]),
                none_to_inf(x.get("status")),
                none_to_inf(x["source"]),
                none_to_inf(x["type"]),
                none_to_blank(x["shipname"]),
                none_to_blank(x["callsign"]),
                none_to_blank(x.get("imo")),
                none_to_blank(x.get("receiver_type")),
                none_to_blank(x.get("receiver")),
            )
        )
        for key, value in self._fragmenter.fragment(messages):
            if key == self.OUTPUT_TAG_MESSAGES:
                yield self._convert_message_out(value)
            elif key == self.OUTPUT_TAG_FRAGMENTS:
                yield TaggedOutput(key, self._convert_fragment_out(value))
            else:
                logger.warning(f"Unknown key in fragment.fragment ({key})")

    def expand(self, xs):
        return xs | FlatMap(self.fragment).with_outputs(main=self.OUTPUT_TAG_MESSAGES)
