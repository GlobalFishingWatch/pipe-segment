import logging
import math

from apache_beam import FlatMap, PTransform
from apache_beam.pvalue import TaggedOutput
from gpsdio_segment.msg_processor import Destination, Identity

from ..tools import datetimeFromTimestamp, timestampFromDatetime
from .fragment_implementation import FragmentImplementation

logger = logging.getLogger(__file__)
logger.setLevel(logging.DEBUG)


def none_to_inf(x):
    return math.inf if (x is None) else x


def make_schema():
    schema = {"fields": []}

    def add_field(name, field_type, mode="REQUIRED"):
        schema["fields"].append(
            dict(
                name=name,
                type=field_type,
                mode=mode,
            )
        )

    add_field("frag_id", "STRING")
    add_field("seg_id", "STRING")
    add_field("ssvid", "STRING")
    add_field("daily_message_count", "INTEGER")
    add_field("timestamp", "TIMESTAMP")
    for prefix in [
        "first_msg_of_day_",
        "last_msg_of_day_",
    ]:
        add_field(prefix + "timestamp", "TIMESTAMP")
        add_field(prefix + "lat", "FLOAT")
        add_field(prefix + "lon", "FLOAT")
        add_field(prefix + "course", "FLOAT", mode="NULLABLE")
        add_field(prefix + "speed", "FLOAT")

    def add_ident_field(name, value_type):
        field = dict(
            name=name,
            type="RECORD",
            mode="REPEATED",
            fields=[dict(name="count", type="INTEGER", mode="NULLABLE")],
        )
        for fld_name in value_type._fields:
            field["fields"].append(dict(name=fld_name, type="STRING", mode="NULLABLE"))
        schema["fields"].append(field)

    add_ident_field("daily_identities", Identity)
    add_ident_field("daily_destinations", Destination)
    add_field("first_timestamp", "TIMESTAMP")
    add_field("cumulative_msg_count", "INTEGER")
    add_ident_field("cumulative_identities", Identity)
    add_ident_field("cumulative_destinations", Destination)

    return schema


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
        msg["timestamp"] = datetimeFromTimestamp(msg["raw_timestamp"])
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
            "first_msg_of_day_timestamp",
            "last_msg_of_day_timestamp",
        ]:
            assert k in frag, frag
            if k in frag and not frag[k] is None:
                frag[k] = timestampFromDatetime(frag[k])
        return frag

    def fragment(self, item):
        _, messages = item
        messages = [self._convert_message_in(x) for x in messages]
        messages.sort(
            key=lambda x: (
                x["timestamp"],
                none_to_inf(x["lon"]),
                none_to_inf(x["lat"]),
                none_to_inf(x["speed"]),
                none_to_inf(x["course"]),
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

    schema = make_schema()
