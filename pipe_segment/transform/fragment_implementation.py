import datetime as dt
import logging
from collections import Counter

import pytz
from gpsdio_segment.segmenter import Segmenter as Fragmenter

logger = logging.getLogger(__file__)
logger.setLevel(logging.INFO)


class FragmentImplementation(object):

    OUTPUT_TAG_MESSAGES = "messages"
    OUTPUT_TAG_FRAGMENTS = "fragments"

    def __init__(self, fragmenter_params=None):
        self.fragmenter_params = fragmenter_params or {}
        assert self.fragmenter_params.get("max_hours", 24) == 24, self.fragmenter_params

    def _fragment_record(self, frag_state, messages, timestamp, signature):

        first_msg_of_day = frag_state.first_msg_of_day
        last_msg_of_day = frag_state.last_msg_of_day

        def idents2record(name):
            items = []
            for k, v in signature.get(name, {}).items():
                value = k._asdict()
                value["count"] = v
                items.append(value)
            return items

        record = dict(
            frag_id=frag_state.id,
            ssvid=frag_state.ssvid,
            noise=frag_state.noise,
            timestamp=timestamp,
            first_msg_timestamp=first_msg_of_day.get("timestamp"),
            first_msg_lat=first_msg_of_day.get("lat"),
            first_msg_lon=first_msg_of_day.get("lon"),
            first_msg_course=first_msg_of_day.get("course"),
            first_msg_speed=first_msg_of_day.get("speed"),
            last_msg_timestamp=last_msg_of_day.get("timestamp"),
            last_msg_lat=last_msg_of_day.get("lat"),
            last_msg_lon=last_msg_of_day.get("lon"),
            last_msg_course=last_msg_of_day.get("course"),
            last_msg_speed=last_msg_of_day.get("speed"),
            msg_count=frag_state.msg_count,
            identities=idents2record("identities"),
            destinations=idents2record("destinations"),
        )
        return record

    @staticmethod
    def _as_datetime(x):
        return dt.datetime.combine(x, dt.time()).replace(tzinfo=pytz.utc)

    def _convert_messages_out(self, msg, frag_id):
        msg = msg.copy()
        msg["frag_id"] = frag_id
        for k1 in ["identities", "destinations"]:
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
        self._update_sig_part(sig, frag.msgs, "destinations")
        return sig

    def _as_record(self, record):
        """Return everything except noise"""
        record = record.copy()
        record.pop("noise")
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

        for frag in Fragmenter(messages, ssvid=ssvid, **self.fragmenter_params):
            if len(frag) > 1:
                logger.debug(
                    f"Got a frag containing {len(frag)} messages, noise={frag.noise}"
                )
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
