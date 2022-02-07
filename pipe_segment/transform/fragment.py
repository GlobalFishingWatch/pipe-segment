import logging

from apache_beam import PTransform
from apache_beam import FlatMap
from apache_beam.pvalue import TaggedOutput
from apache_beam.io.gcp.internal.clients import bigquery

from ..timestamp import datetimeFromTimestamp
from ..timestamp import timestampFromDatetime

from .fragment_implementation import FragmentImplementation
from gpsdio_segment.msg_processor import Identity


logger = logging.getLogger(__file__)
logger.setLevel(logging.DEBUG)


class Fragment(PTransform):
    def __init__(
        self,
        start_date=None,
        end_date=None,
        fragmenter_params=None,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self._fragmenter = FragmentImplementation(
            start_date, end_date, fragmenter_params
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
            # "first_msg_of_day_timestamp",
            # "last_msg_of_day_timestamp",
            # "timestamp_first",
            # "timestamp_last",  # Stats stuff TODO: clean out someday
            # "timestamp_min",
            # "timestamp_max",
        ]:
            assert k in frag, frag
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

        # TODO: for NESTED remove is we go with flat
        # def add_ident_field(name, value_type):
        #     field = bigquery.TableFieldSchema()
        #     field.name = name
        #     field.type = "RECORD"
        #     field.mode = "REPEATED"
        #     fc = bigquery.TableFieldSchema()
        #     fc.name = "count"
        #     fc.type = "INTEGER"
        #     fc.mode = "REQUIRED"
        #     fv = bigquery.TableFieldSchema()
        #     fv.name = "value"
        #     fv.type = "RECORD"
        #     fv.mode = "REQUIRED"
        #     fields = []
        #     for fld_name in value_type._fields:
        #         f = bigquery.TableFieldSchema()
        #         f.name = fld_name
        #         f.type = "STRING"
        #         f.mode = "NULLABLE"
        #         fields.append(f)
        #     fv.fields = fields
        #     field.fields = [fc, fv]
        #     schema.fields.append(field)

        # def add_ident_field(name, value_type):
        #     field = bigquery.TableFieldSchema()
        #     field.name = name
        #     field.type = "RECORD"
        #     field.mode = "REPEATED"
        #     fields = []
        #     for fld_name in value_type._fields:
        #         f = bigquery.TableFieldSchema()
        #         f.name = fld_name
        #         f.type = "STRING"
        #         f.mode = "NULLABLE"
        #         fields.append(f)
        #     f = bigquery.TableFieldSchema()
        #     f.name = "COUNT"
        #     f.type = "INTEGER"
        #     fields.append(f)
        #     field.fields = fields
        #     schema.fields.append(field)

        def add_ident_field(name, value_type):
            field = dict(
                name=name,
                type="RECORD",
                mode="REPEATED",
                fields=[dict(name="count", type="INTEGER", mode="NULLABLE")],
            )
            for fld_name in value_type._fields:
                field["fields"].append(
                    dict(name=fld_name, type="STRING", mode="NULLABLE")
                )
            schema["fields"].append(field)

        add_ident_field("identities", Identity)

        return schema
