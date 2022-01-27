import logging

from apache_beam import PTransform
from apache_beam import FlatMap
from apache_beam.pvalue import TaggedOutput
from apache_beam.io.gcp.internal.clients import bigquery

from ..timestamp import datetimeFromTimestamp
from ..timestamp import timestampFromDatetime

from .fragment_implementation import FragmentImplementation

logger = logging.getLogger(__file__)
logger.setLevel(logging.DEBUG)


class Fragment(PTransform):
    def __init__(
        self,
        start_date=None,
        end_date=None,
        fragmenter_params=None,
        stats_fields=None,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self._fragmenter = FragmentImplementation(
            start_date, end_date, stats_fields, fragmenter_params
        )

    @property
    def OUTPUT_TAG_FRAGMENTS(self):
        return self._fragmenter.OUTPUT_TAG_FRAGMENTS

    @property
    def OUTPUT_TAG_MESSAGES(self):
        return self._fragmenter.OUTPUT_TAG_MESSAGES

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

    # TODO: do we use this? Doing this daily I don't think so
    @staticmethod
    def _convert_fragment_in(frag):
        frag = dict(frag.items())
        for k in [
            "timestamp",
            "first_msg_timestamp",
            "last_msg_timestamp",
            "first_msg_of_day_timestamp",
            "last_msg_of_day_timestamp",
        ]:
            if frag[k] is not None:
                frag[k] = datetimeFromTimestamp(frag[k])
        return frag

    @staticmethod
    def _convert_fragment_out(frag):
        frag = dict(frag.items())
        for k in [
            "timestamp",
            "first_msg_timestamp",
            "last_msg_timestamp",
            "first_msg_of_day_timestamp",
            "last_msg_of_day_timestamp",
            "timestamp_first",
            "timestamp_last",  # Stats stuff TODO: clean out someday
            "timestamp_min",
            "timestamp_max",
        ]:
            if k in frag and not frag[k] is None:
                frag[k] = timestampFromDatetime(frag[k])
        return frag

    def fragment(self, item):
        _, messages = item
        messages = [self._convert_message_in(x) for x in messages]
        messages.sort(key=lambda x: x["timestamp"])
        logger.debug(
            f"Fragmenting sorted {len(messages)} messages",
        )
        for key, value in self._fragmenter.fragment(messages):
            if key == self.OUTPUT_TAG_MESSAGES:
                msg = self._convert_message_out(value)
                yield msg
            elif key == self.OUTPUT_TAG_FRAGMENTS:
                yield TaggedOutput(key, self._convert_fragment_out(value))
            else:
                logger.warning(f"Unknown key in fragment.fragment ({key})")

    def expand(self, xs):
        return xs | FlatMap(self.fragment).with_outputs(main=self.OUTPUT_TAG_MESSAGES)

    @property
    def fragment_schema(self):
        schema = bigquery.TableSchema()

        def add_field(name, field_type, mode="REQUIRED"):
            field = bigquery.TableFieldSchema()
            field.name = name
            field.type = field_type
            field.mode = mode
            schema.fields.append(field)

        add_field("frag_id", "STRING")
        add_field("ssvid", "STRING")
        add_field("message_count", "INTEGER")
        add_field("timestamp", "TIMESTAMP")
        for prefix in [
            "first_msg_",
            "last_msg_",
        ]:
            mode = "NULLABLE" if prefix.endswith("of_day_") else "REQUIRED"
            add_field(prefix + "timestamp", "TIMESTAMP", mode)
            add_field(prefix + "lat", "FLOAT", mode)
            add_field(prefix + "lon", "FLOAT", mode)
            add_field(prefix + "course", "FLOAT", mode)
            add_field(prefix + "speed", "FLOAT", mode)

        def add_sig_field(name, value_type="STRING"):
            field = bigquery.TableFieldSchema()
            field.name = name
            field.type = "RECORD"
            field.mode = "REPEATED"
            f1 = bigquery.TableFieldSchema()
            f1.name = "value"
            f1.type = value_type
            f2 = bigquery.TableFieldSchema()
            f2.name = "count"
            f2.type = "INTEGER"
            field.fields = [f1, f2]
            schema.fields.append(field)

        add_sig_field("shipnames")
        add_sig_field("callsigns")
        add_sig_field("imos")
        add_sig_field("destinations")
        add_sig_field("lengths", value_type="FLOAT")
        add_sig_field("widths", value_type="FLOAT")
        add_sig_field("transponders")

        return schema
