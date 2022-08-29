from collections import defaultdict

import apache_beam as beam
from shipdataprocess.normalize import normalize_callsign, normalize_shipname
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
            result.append(
                {
                    "count": x.get("count"),
                    "value": value,
                }
            )

    return result


def extract(identities, key):
    mapping = defaultdict(int)
    for ident in identities:
        value = ident[key]
        if value is not None:
            mapping[value] += ident["count"]
    return [{"value": k, "count": v} for (k, v) in mapping.items()]

def summarize_identifiers(segment):
    identities = segment["daily_identities"]
    counts = [ident["count"] for ident in identities]
    transponders = extract(identities, "transponder_type")
    shipnames = extract(identities, "shipname")
    callsigns = extract(identities, "callsign")
    imos = extract(identities, "imo")
    lengths = extract(identities, "length")
    widths = extract(identities, "width")

    return {
        "seg_id": segment.get("seg_id"),
        "ssvid": segment.get("ssvid"),
        "timestamp": segment.get("timestamp"),
        "first_timestamp": segment.get("first_msg_timestamp"),
        "last_timestamp": segment.get("last_msg_timestamp"),
        "first_pos_timestamp": segment.get("first_msg_timestamp"),
        "last_pos_timestamp": segment.get("last_msg_timestamp"),
        "msg_count": segment.get("daily_msg_count") + sum(counts),
        "pos_count": segment.get("daily_msg_count"),
        # We approximate identity message count by summing all the counts from
        # the atomic identity messages we collected in the segment.
        "ident_count": sum(counts),
        "shipname": shipnames or None,
        "callsign": callsigns or None,
        "imo": imos or None,
        "n_shipname": normalize_counted_array(normalize_shipname, shipnames) or None,
        "n_callsign": normalize_counted_array(normalize_callsign, callsigns) or None,
        "n_imo": normalize_counted_array(normalize_imo, imos) or None,
        "shiptype": None,
        "length": lengths or None,
        "width": widths or None,
        "noise": False,
    }


def rename_timestamp(record):
    result = record.copy()
    result["summary_timestamp"] = result.pop("timestamp")
    return result
    

BQ_PARAMS = {
    "destinationTableProperties": {
        "description": "Daily satellite messages.",
    },
}


def write_sink(sink_table, schema, from_dt, description):
    sink_table = sink_table.replace("bq://", "")
    bq_params_cp = dict(BQ_PARAMS)
    bq_params_cp["destinationTableProperties"]["description"] = description

    def compute_table(message):
        timestamp = message["summary_timestamp"]
        return f"{sink_table}{timestamp:%Y%m%d}"

    return beam.io.WriteToBigQuery(
        compute_table,
        schema=schema,
        additional_bq_parameters=bq_params_cp,
        write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
        create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
    )
