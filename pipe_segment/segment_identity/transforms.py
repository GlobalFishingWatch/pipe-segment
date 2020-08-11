from functools import reduce

from apache_beam.transforms.window import TimestampedValue

from shipdataprocess.normalize import normalize_callsign
from shipdataprocess.normalize import normalize_shipname
from stdnum import imo as imo_validator

def normalize_imo(value):
    if imo_validator.is_valid(str(value)):
        return value
    else:
        return None

def extract_count(x):
    return x.get("count", 0)

def normalize_counted_array(f, xs):
    result = []
    for x in xs:
        value = f(x.get("value"))
        if value is not None:
            result.append({
                "count": x.get("count"),
                "value": value,
            })

    return result

def summarize_identifiers(segment):
    transponders = segment.get("transponders", [])
    shipnames = segment.get("shipnames", [])
    callsigns = segment.get("callsigns", [])
    imos = segment.get("imos", [])

    return {
        "seg_id": segment.get("seg_id"),
        "ssvid": segment.get("ssvid"),
        "timestamp": segment.get("timestamp"),
        "first_timestamp": segment.get("first_msg_of_day_timestamp"),
        "last_timestamp": segment.get("last_msg_of_day_timestamp"),
        "first_pos_timestamp": segment.get("first_msg_of_day_timestamp"),
        "last_pos_timestamp": segment.get("last_msg_of_day_timestamp"),
        "msg_count": segment.get("message_count"),
        # We approximate positional message counts by summing all the counts
        # from all the diferent transponder values we collected in the segment.
        "pos_count": sum(map(extract_count, transponders)),
        # We approximate identity message count by summing all the counts from
        # all the different shipname values we collected.
        "ident_count": sum(map(extract_count, shipnames)),
        "shipname": shipnames or None,
        "callsign": callsigns or None,
        "imo": imos or None,
        "n_shipname": normalize_counted_array(normalize_shipname, shipnames) or None,
        "n_callsign": normalize_counted_array(normalize_callsign, callsigns) or None,
        "n_imo": normalize_counted_array(normalize_imo, imos) or None,
        "shiptype": None,
        "length": None,
        "width": None,
        "noise": False
    }

def to_timestamped_value(record):
    result = record.copy()
    timestamp = result.pop("timestamp")

    return TimestampedValue(result, timestamp)
