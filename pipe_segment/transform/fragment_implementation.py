from collections import Counter
import datetime as dt
import logging
import pytz

from pipe_segment.stats import MessageStats

# TODO: what is the new gpsdio segment name here?
from gpsdio_segment.segmenter import Segmenter as Fragmenter

logger = logging.getLogger(__file__)
logger.setLevel(logging.INFO)


class FragmentImplementation(object):

    OUTPUT_TAG_MESSAGES = "messages"
    OUTPUT_TAG_FRAGMENTS = "fragments"

    # TODO: remove? Need it for first and last message but could get that cleaner
    DEFAULT_STATS_FIELDS = [
        ("lat", MessageStats.NUMERIC_STATS),
        ("lon", MessageStats.NUMERIC_STATS),
        ("timestamp", MessageStats.NUMERIC_STATS),
        ("shipname", MessageStats.FREQUENCY_STATS),
        ("imo", MessageStats.FREQUENCY_STATS),
        ("callsign", MessageStats.FREQUENCY_STATS),
    ]

    def __init__(
        self,
        start_date=None,
        end_date=None,
        # stats_fields=None,
        fragmenter_params=None,
    ):
        self.start_date = start_date
        self.end_date = end_date
        self.stats_fields = self.DEFAULT_STATS_FIELDS
        self.fragmenter_params = fragmenter_params or {}
        assert self.fragmenter_params.get("max_hours") == 24, self.fragmenter_params

    @staticmethod
    def stat_output_field_name(field_name, stat_name):
        return "%s_%s" % (field_name, stat_name)

    def _fragment_record(self, frag_state, messages, timestamp, signature):

        stats_numeric_fields = [
            f
            for f, stats in self.stats_fields
            if set(stats) & set(MessageStats.NUMERIC_STATS)
        ]
        stats_frequency_fields = [
            f
            for f, stats in self.stats_fields
            if set(stats) & set(MessageStats.FREQUENCY_STATS)
        ]
        # has_timestamp = "timestamp" in stats_numeric_fields
        stats_numeric_fields = [x for x in stats_numeric_fields if x != "timestamp"]
        ms = MessageStats(messages, stats_numeric_fields, stats_frequency_fields)

        first_msg = frag_state.first_msg
        last_msg = frag_state.last_msg
        first_msg_of_day = frag_state.first_msg_of_day or {}
        last_msg_of_day = frag_state.last_msg_of_day or {}

        def idents2record(name):
            items = []
            for k, v in signature.get(name, {}).items():
                value = k._asdict()
                value["count"] = v
                items.append(value)
                # TODO: for NESTED remove is we go with flat
                # items.append({"value": value, "count": v})
            return items

        record = dict(
            frag_id=frag_state.id,
            ssvid=frag_state.ssvid,
            noise=frag_state.noise,
            message_count=frag_state.msg_count,
            first_msg_timestamp=first_msg["timestamp"],
            first_msg_lat=first_msg["lat"],
            first_msg_lon=first_msg["lon"],
            first_msg_course=first_msg["course"],
            first_msg_speed=first_msg["speed"],
            last_msg_timestamp=last_msg["timestamp"],
            last_msg_lat=last_msg["lat"],
            last_msg_lon=last_msg["lon"],
            last_msg_course=last_msg["course"],
            last_msg_speed=last_msg["speed"],
            first_msg_of_day_timestamp=first_msg_of_day.get("timestamp"),
            first_msg_of_day_lat=first_msg_of_day.get("lat"),
            first_msg_of_day_lon=first_msg_of_day.get("lon"),
            first_msg_of_day_course=first_msg_of_day.get("course"),
            first_msg_of_day_speed=first_msg_of_day.get("speed"),
            last_msg_of_day_timestamp=last_msg_of_day.get("timestamp"),
            last_msg_of_day_lat=last_msg_of_day.get("lat"),
            last_msg_of_day_lon=last_msg_of_day.get("lon"),
            last_msg_of_day_course=last_msg_of_day.get("course"),
            last_msg_of_day_speed=last_msg_of_day.get("speed"),
            timestamp=timestamp,
            identities=idents2record("identities"),
        )
        for field, stats in self.stats_fields:
            stat_values = ms.field_stats(field)
            for stat in stats:
                record[self.stat_output_field_name(field, stat)] = stat_values.get(
                    stat, None
                )
        # if has_timestamp:
        #     if len(messages):
        #         record["timestamp_min"] = messages[0]["timestamp"]
        #         record["timestamp_max"] = messages[-1]["timestamp"]
        #         record["timestamp_first"] = messages[0]["timestamp"]
        #         record["timestamp_last"] = messages[-1]["timestamp"]
        #     else:
        #         record["timestamp_min"] = record["timestamp_max"] = None
        #         record["timestamp_first"] = record["timestamp_last"] = None
        #     record["timestamp_count"] = len(messages)
        return record

    @staticmethod
    def _as_datetime(x):
        return dt.datetime.combine(x, dt.time()).replace(tzinfo=pytz.utc)

    def _convert_messages_out(self, msg, frag_id):
        msg = msg.copy()
        msg["frag_id"] = frag_id
        for k1 in [
            "identities",
        ]:
            msg.pop(k1, None)
        return msg

    @staticmethod
    def _update_sig_part(sig, msgs, key):
        counter = Counter(sig.get(key, {}))
        for m in msgs:
            for k, cnt in m[key].items():
                counter.update({k: cnt})
        sig[key] = dict(counter)

    def _get_signature(self, frag, date):
        sig = {}
        self._update_sig_part(sig, frag.msgs, "identities")
        # TODO: add stuff to identities in GPSDIO segment (length, width, type)
        # TODO: add destinations after implemented in GPSDIO segment
        return sig

    def _as_record(self, record):
        """Return everything except noise"""
        record = record.copy()
        record.pop("noise")
        # TODO: eventually don't generate these at all.
        for field, stats in self.stats_fields:
            for stat in stats:
                record.pop(self.stat_output_field_name(field, stat))
        for end in ["first", "last"]:
            for name in ["timestamp", "lat", "lon", "course", "speed"]:
                record.pop(f"{end}_msg_of_day_{name}")
        return record

    def fragment(self, messages):
        dates = set(msg["timestamp"].date() for msg in messages)
        if len(dates) != 1:
            raise RuntimeError("fragment expects all messages from a single date")
        [date] = dates
        timestamp = self._as_datetime(date)
        ssvids = set(msg["ssvid"] for msg in messages)
        if len(ssvids) != 1:
            raise RuntimeError("fragment expects all messages from a single SSVID")
        [ssvid] = ssvids

        assert self.fragmenter_params.get("max_hours") == 24, self.fragmenter_params
        for frag in Fragmenter(messages, ssvid=ssvid, **self.fragmenter_params):
            if not frag.noise:
                frag.msgs = [x for x in frag.msgs if x["timestamp"].date() <= date]
                if frag.msgs or frag.prev_state:
                    logger.debug(
                        "Fragmenting key %r yielding fragment %s containing %s messages "
                        % (frag.ssvid, frag.id, len(frag))
                    )
                    signature = self._get_signature(frag, date)
                    rcd = self._fragment_record(
                        frag.state, frag.msgs, timestamp, signature
                    )
                    output_rcd = rcd.copy()
                    output_rcd["timestamp"] = timestamp
                    # Only store new style records, so that we get the ~same code path
                    # running over multiple days as running over a single day.
                    output_rcd = self._as_record(output_rcd)
                    yield (self.OUTPUT_TAG_FRAGMENTS, output_rcd)
            frag_id = None if frag.noise else frag.id
            for msg in frag:
                msg = self._convert_messages_out(msg, frag_id)
                yield (self.OUTPUT_TAG_MESSAGES, msg)
