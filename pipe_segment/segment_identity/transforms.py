from collections import defaultdict

import apache_beam as beam
from shipdataprocess.normalize import normalize_callsign, normalize_shipname
from stdnum import imo as imo_validator


def normalize_imo(value):
    if imo_validator.is_valid(str(value)):
        return value
    else:
        return None


def always_true(x):
    return True


def no_normalization(x):
    return x


def extract_identity_counts(
        identities,
        key,
        filtering_fn=always_true,
        normalization_fn=no_normalization):
    # Every value starts at zero
    accummulator = defaultdict(int)

    for identity in identities:
        value = identity[key]
        count = identity["count"]

        if filtering_fn(value):
            normalized_value = normalization_fn(value)
            accummulator[normalized_value] += count

    result = [{"value": k, "count": v} for (k, v) in accummulator.items()]
    return result or None


def summarize_identifiers(segment):
    identities = segment["daily_identities"]
    pos_count = segment.get("daily_msg_count")
    # We approximate identity message count by summing all the counts from
    # the atomic identity messages we collected in the segment.
    counts = [ident["count"] for ident in identities]
    ident_count = sum(counts)
    msg_count = pos_count + ident_count

    return {
        "seg_id": segment.get("seg_id"),
        "ssvid": segment.get("ssvid"),
        "timestamp": segment.get("timestamp"),
        "first_timestamp": segment.get("first_msg_timestamp"),
        "last_timestamp": segment.get("last_msg_timestamp"),
        "first_pos_timestamp": segment.get("first_msg_timestamp"),
        "last_pos_timestamp": segment.get("last_msg_timestamp"),
        "msg_count": msg_count,
        "pos_count": pos_count,
        "ident_count": ident_count,
        "shipname": extract_identity_counts(
            identities,
            key="shipname",
            filtering_fn=normalize_shipname,
        ),
        "callsign": extract_identity_counts(
            identities,
            key="callsign",
            filtering_fn=normalize_callsign,
        ),
        "imo": extract_identity_counts(
            identities,
            key="imo",
            filtering_fn=normalize_imo,
        ),
        "n_shipname": extract_identity_counts(
            identities,
            key="shipname",
            filtering_fn=normalize_shipname,
            normalization_fn=normalize_shipname,
        ),
        "n_callsign": extract_identity_counts(
            identities,
            key="callsign",
            filtering_fn=normalize_callsign,
            normalization_fn=normalize_callsign,
        ),
        "n_imo": extract_identity_counts(
            identities,
            key="imo",
            filtering_fn=normalize_imo,
            normalization_fn=normalize_imo,
        ),
        "length": extract_identity_counts(
            identities,
            key="length",
            filtering_fn=lambda x: x,  # Discard None
        ),
        "width": extract_identity_counts(
            identities,
            key="width",
            filtering_fn=lambda x: x,  # Discard None
        ),
    }


def rename_timestamp(record):
    result = record.copy()
    result["summary_timestamp"] = result.pop("timestamp")
    return result


def write_sink(sink_table, schema):
    def compute_table(message):
        timestamp = message["summary_timestamp"]
        return f"{sink_table}{timestamp:%Y%m%d}"

    return beam.io.WriteToBigQuery(
        compute_table,
        schema={"fields": schema},
        write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
        create_disposition=beam.io.BigQueryDisposition.CREATE_NEVER,
    )
