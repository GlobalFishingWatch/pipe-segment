from functools import reduce

from apache_beam.transforms.window import TimestampedValue
from pipe_tools.utils.timestamp import as_timestamp

from shipdataprocess.normalize import normalize_callsign
from shipdataprocess.normalize import normalize_shipname
from stdnum import imo as imo_validator

import apache_beam as beam

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
    timestamp = as_timestamp(result.pop("timestamp"))

    return TimestampedValue(result, timestamp)


SOURCE_QUERY_TEMPLATE = """
    SELECT
      *
    FROM
      `{source_table}*`
    WHERE
      _TABLE_SUFFIX BETWEEN FORMAT_TIMESTAMP('%Y%m%d', TIMESTAMP_SECONDS({start_ts}))
      AND FORMAT_TIMESTAMP('%Y%m%d', TIMESTAMP_SECONDS({end_ts}))
      AND TRUE
"""

class ReadSource(beam.PTransform):
    def __init__(self, source_table, start_ts, end_ts):
        self.source_table = source_table.replace('bq://','').replace(':', '.')
        self.start_ts = int(start_ts)
        self.end_ts = int(end_ts)

    def read_source(self):
        query = SOURCE_QUERY_TEMPLATE.format(
            source_table = self.source_table,
            start_ts = self.start_ts,
            end_ts = self.end_ts,
        )
        return beam.io.ReadFromBigQuery(
            query = query,
            use_standard_sql = True,
        )

    def expand(self, pcoll):
        return pcoll | self.read_source()

BQ_PARAMS = {
    "destinationTableProperties": {
        "description": "Daily satellite messages.",
    },
}

def write_sink(sink_table, schema, from_dt, to_dt, description):
    sink_table = sink_table.replace('bq://','')
    bq_params_cp = dict(BQ_PARAMS)
    bq_params_cp['destinationTableProperties']['description'] = description

    def compute_table(message):
        # due this is for counting identity data
        # if no last_timestamp, save in at the end of the boundary to_dt if has first_timestamp, if not at the start.
        separator_ts=(message['last_timestamp'] if message['last_timestamp'] else (to_dt if message['first_timestamp'] else from_dt))
        table_suffix = separator_ts.strftime("%Y%m%d")
        return "{}{}".format(sink_table, table_suffix)

    return beam.io.WriteToBigQuery(
        compute_table,
        schema=schema,
        additional_bq_parameters=bq_params_cp,
        write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
        create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
    )
