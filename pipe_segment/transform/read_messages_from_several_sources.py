import copy
from apache_beam.io.gcp.internal.clients.bigquery import TableFieldSchema

from datetime import datetime, timedelta

from pipe_segment.transform.read_source import ReadSource

from pipe_tools.io.bigquery import parse_table_schema
from pipe_tools.utils.timestamp import as_timestamp

import apache_beam as beam

PAD_PREV_DAYS = 1

def is_first_batch(pipeline_start, first_date):
    return pipeline_start == first_date

def offset_timestamp(dt, **timedelta_args):
    if dt is None:
        return dt
    dt_delta = dt + timedelta(**timedelta_args)
    return dt_delta


class ReadMessagesFromSeveralSources(beam.PTransform):
    def __init__(self, options):
        self.options = options
        datetimeFromStr = lambda x: datetime.strptime(x, '%Y-%m-%d')
        extract_range = lambda s: list(map(datetimeFromStr, s.split(','))) if s is not None else (None, None)
        self.date_range = extract_range(self.options.date_range)
        self.pipeline_start_date_dt = datetimeFromStr(self.options.pipeline_start_date)
        self._message_sources_list = None
        self._message_output_schema = None

    @property
    def message_sources_list(self):
        # message_sources_list is set only once and has ReadSource of each source.
        if not self._message_sources_list:
            first_date_dt, last_date_dt = self.date_range

            #If the first_date_dt is the first day of data, then, don't look back
            if not is_first_batch(self.pipeline_start_date_dt, first_date_dt):
                first_date_dt = offset_timestamp(first_date_dt, days=-PAD_PREV_DAYS)

            if self.options.look_ahead:
                last_date_dt = offset_timestamp(last_date_dt, days=self.options.look_ahead)

            first_date_ts, last_date_ts = list(map(as_timestamp, [first_date_dt, last_date_dt]))
            self._message_sources_list = [ReadSource(src_msg_table, first_date_ts, last_date_ts) for src_msg_table in self.options.source.split(',')]
        return self._message_sources_list

    def read_from_several_sources(self, pcoll):
        compose = lambda x,source: pcoll | f"Source{x}" >> source
        return [compose(idx, source) for idx, source in enumerate(self.message_sources_list)]

    @property
    def message_input_schema(self):
        schema = self.options.source_schema
        if schema is None:
            # no explicit schema provided. Try to find one in the source(s)
            schemas = [s.schema for s in self.message_sources_list if s.schema is not None]
            schema = schemas[0] if schemas else None
        return parse_table_schema(schema)

    @property
    def message_output_schema(self):
        if not self._message_output_schema:
            schema = copy.deepcopy(self.message_input_schema)

            # Ensure to set all fields mode None as NULLABLE
            for f in schema.fields:
                if f.mode==None:
                    f.mode="NULLABLE"

            field = TableFieldSchema()
            field.name = "seg_id"
            field.type = "STRING"
            field.mode="NULLABLE"
            schema.fields.append(field)

            field = TableFieldSchema()
            field.name = "n_shipname"
            field.type = "STRING"
            field.mode="NULLABLE"
            schema.fields.append(field)

            field = TableFieldSchema()
            field.name = "n_callsign"
            field.type = "STRING"
            field.mode="NULLABLE"
            schema.fields.append(field)

            field = TableFieldSchema()
            field.name = "n_imo"
            field.type = "INTEGER"
            field.mode="NULLABLE"
            schema.fields.append(field)

            self._message_output_schema = schema
        return self._message_output_schema

    def expand(self, pcoll):
        return (
            self.read_from_several_sources(pcoll)
            | "MergeMessages" >> beam.Flatten()
        )

