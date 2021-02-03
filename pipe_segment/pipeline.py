import logging
import ujson
from datetime import timedelta
from datetime import datetime
from apitools.base.py.exceptions import HttpError
import pytz

import apache_beam as beam
from apache_beam.runners import PipelineState
from apache_beam.io.gcp.internal.clients.bigquery import TableFieldSchema
from apache_beam.options.pipeline_options import GoogleCloudOptions
from apache_beam.options.pipeline_options import StandardOptions
from apache_beam.transforms.window import TimestampedValue
from apache_beam.io.gcp.internal.clients import bigquery

from pipe_tools.timestamp import TimestampedValueDoFn
from pipe_tools.timestamp import datetimeFromTimestamp
from pipe_tools.timestamp import timestampFromDatetime
from pipe_tools.utils.timestamp import as_timestamp
from pipe_tools.io.bigquery import parse_table_schema
from pipe_tools.io import WriteToBigQueryDatePartitioned

from pipe_segment.options.segment import SegmentOptions
from pipe_segment.transform.segment import Segment
from pipe_segment.transform.satellite_offsets import SatelliteOffsets
from pipe_segment.transform.normalize import NormalizeDoFn
from pipe_segment.io.gcp import GCPSource
from pipe_segment.io.gcp import GCPSink

PAD_PREV_DAYS = 1

def safe_dateFromTimestamp(ts):
    if ts is None:
        return None
    return datetimeFromTimestamp(ts).date()

def parse_date_range(s):
    # parse a string YYYY-MM-DD,YYYY-MM-DD into 2 timestamps
    return list(map(as_timestamp, s.split(',')) if s is not None else (None, None))

def offset_timestamp(ts, **timedelta_args):
    if ts is None:
        return ts
    dt = datetimeFromTimestamp(ts) + timedelta(**timedelta_args)
    return timestampFromDatetime(dt)

def is_first_batch(pipeline_start_ts, first_date_ts):
    return pipeline_start_ts == first_date_ts

def filter_by_ssvid_predicate(obj, valid_ssvid_set):
    return obj['ssvid'] in valid_ssvid_set

class LogMapper(object):
    first_item = True
    which_item = 0

    def log_first_item(self, obj):
        if self.first_item:
            logging.warn("First Item: %s", obj)
            self.first_item = False
        return obj

    def log_nth_item(self, obj, n):
        if self.which_item == n:
            logging.warn("%sth Item: %s", n, obj)
            self.which_item += 1
        return obj

    def log_first_item_keys(self, obj):
        if self.first_item:
            logging.warn("First Item's Keys: %s", obj.keys())
            self.first_item = False
        return obj


def make_sat_key(receiver, timestamp, offset=0):
    dt = datetimeFromTimestamp(timestamp)
    hour = dt.hour + offset
    return f'{receiver}-{dt.year}-{dt.month}-{dt.day}-{hour}'

def make_offset_sat_key_set(msg, max_offset):
    for offset in range(-max_offset, max_offset + 1):
        yield make_sat_key(msg["receiver"], msg['hour'], offset)

def not_during_bad_hour(msg, bad_hours):
    return make_sat_key(msg["receiver"], msg['timestamp']) not in bad_hours

def greater_than_max_dt(x, max_dt):
    return abs(x['dt']) > max_dt

class SegmentPipeline:
    def __init__(self, options):
        self.options = options.view_as(SegmentOptions)
        self.date_range = parse_date_range(self.options.date_range)
        self._message_source_list = None

    @property
    def sat_offset_schema(self):
        schema = bigquery.TableSchema()

        def add_field(name, field_type, mode='REQUIRED'):
            field = bigquery.TableFieldSchema()
            field.name = name
            field.type = field_type
            field.mode = mode
            schema.fields.append(field)

        add_field('hour', 'timestamp')
        add_field('receiver',  'STRING')
        add_field('dt', 'FLOAT')
        add_field('pings', 'INTEGER')
        add_field('avg_distance_from_sat_km', 'FLOAT')
        add_field('med_dist_from_sat_km', 'FLOAT')

        return schema

    @property
    def sat_offset_sink(self):
        sink_table = self.options.sat_offset_dest
        if not sink_table.startswith('bq://'):
            raise ValueError('only BigQuery supported as a destination for `sat_offset_dest, must begine with "bq://"')
        sink_table = sink_table[5:]
        if ':' not in sink_table:
          sink_table = self.cloud_options.project + ':' + sink_table
        print(sink_table)
        sink = WriteToBigQueryDatePartitioned(table=sink_table,
                       schema=self.sat_offset_schema,
                       temp_gcs_location=self.temp_gcs_location,
                       temp_shards_per_day=self.options.temp_shards_per_day,
                       write_disposition="WRITE_TRUNCATE")
        return sink

    @property
    def message_source_list(self):
        # creating a GCPSource requires calls to the bigquery API if we are
        # reading from bigquery, so only do this once.
        if not self._message_source_list:
            first_date_ts, last_date_ts = self.date_range
            #If the first_date_ts is the first day of data, then, don't look back
            if not is_first_batch(as_timestamp(self.options.pipeline_start_date),first_date_ts):
                first_date_ts = offset_timestamp(first_date_ts, days=-PAD_PREV_DAYS)
            if self.options.look_ahead:
                last_date_ts = offset_timestamp(last_date_ts, days=self.options.look_ahead)
            gcp_paths = self.options.source.split(',')
            self._message_source_list = []
            for gcp_path in gcp_paths:
                s = GCPSource(gcp_path=gcp_path,
                               first_date_ts=first_date_ts,
                               last_date_ts=last_date_ts)
                self._message_source_list.append(s)

        return self._message_source_list

    def message_sources(self, pipeline):
        def compose(idx, source):
            return pipeline | "Source%i" % idx >> source

        return (compose (idx, source) for idx, source in enumerate(self.message_source_list))


    @property
    def message_input_schema(self):
        schema = self.options.source_schema
        if schema is None:
            # no explicit schema provided. Try to find one in the source(s)
            schemas = [s.schema for s in self.message_source_list if s.schema is not None]
            schema = schemas[0] if schemas else None
        return parse_table_schema(schema)

    @property
    def message_output_schema(self):
        schema = self.message_input_schema

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

        return schema


    @property
    def segmenter_params(self):
        return ujson.loads(self.options.segmenter_params)

    @property
    def temp_gcs_location(self):
        return self.options.view_as(GoogleCloudOptions).temp_location

    @staticmethod
    def groupby_fn(msg):
        return (msg['ssvid'], msg)

    @property
    def message_sink(self):
        sink = GCPSink(gcp_path=self.options.msg_dest,
                       schema=self.message_output_schema,
                       temp_gcs_location=self.temp_gcs_location,
                       temp_shards_per_day=self.options.temp_shards_per_day)
        return sink

    @property
    def segment_source(self):
        if self.date_range[0] is None:
            return beam.Create([])

        dt = datetimeFromTimestamp(self.date_range[0])
        ts = timestampFromDatetime(dt - timedelta(days=1))

        try:
            source = GCPSource(gcp_path=self.options.seg_dest,
                             first_date_ts=ts,
                             last_date_ts=ts)
        except HttpError as exn:
            logging.warn("Segment source not found: %s %s" % (self.options.seg_dest, dt))
            if exn.status_code == 404:
                return beam.Create([])
            else:
                raise
        return source


    def segment_sink(self, schema, table):
        sink = GCPSink(gcp_path=table,
                       schema=schema,
                       temp_gcs_location=self.temp_gcs_location,
                       temp_shards_per_day=self.options.temp_shards_per_day)
        return sink


    def pipeline(self):
        # Note that Beam appears to treat str(x) and unicode(x) as distinct
        # for purposes of CoGroupByKey, so both messages and segments should be
        # stringified or neither. 
        pipeline = beam.Pipeline(options=self.options)

        start_date = safe_dateFromTimestamp(self.date_range[0])
        end_date = safe_dateFromTimestamp(self.date_range[1])

        satellite_offsets = pipeline | SatelliteOffsets(start_date, end_date)

        ( satellite_offsets
            | "AddTimestamp" >> beam.Map(lambda x: TimestampedValue(x, x['hour']))
            | "WriteSatOffsets" >> self.sat_offset_sink
        )

        bad_satellite_hours = (
            satellite_offsets
            | beam.Filter(greater_than_max_dt, max_dt=self.options.max_timing_offset_s)
            | beam.FlatMap(make_offset_sat_key_set, max_offset=self.options.bad_hour_padding)
        )

        bad_hours = beam.pvalue.AsDict(bad_satellite_hours | beam.Map(lambda x : (x, x)))

        messages = (
            self.message_sources(pipeline)
            | "MergeMessages" >> beam.Flatten()
            | "FilterBadTimes" >> beam.Filter(not_during_bad_hour, bad_hours)
        )


        if self.options.ssvid_filter_query:
            # TODO: does this work?
            valid_ssivd_set = set(beam.pvalue.AsIter(
                messages
                | GCPSource(gcp_path=self.options.ssvid_filter_query)
                | beam.Map(lambda x: (x['ssvid']))
                ))
            messages = (
                messages
                | beam.Filter(filter_by_ssvid_predicate, valid_ssivd_set)
            )

        messages = (   
            messages
            | "Normalize" >> beam.ParDo(NormalizeDoFn())
            | "MessagesAddKey" >> beam.Map(self.groupby_fn)
        )

        segments = (
            pipeline
            | "ReadSegments" >> self.segment_source
            | "RemoveClosedSegments" >> beam.Filter(lambda x: not x['closed'])
            | "SegmentsAddKey" >> beam.Map(self.groupby_fn)
        )

        args = (
            {'messages' : messages, 'segments' : segments}
            | 'GroupByKey' >> beam.CoGroupByKey()
        )

        segmenter = Segment(start_date=safe_dateFromTimestamp(self.date_range[0]),
                            end_date=safe_dateFromTimestamp(self.date_range[1]),
                            segmenter_params=self.segmenter_params, 
                            look_ahead=self.options.look_ahead)

        segmented = args | "Segment" >> segmenter

        messages = segmented[segmenter.OUTPUT_TAG_MESSAGES]
        segments = segmented[segmenter.OUTPUT_TAG_SEGMENTS]
        (
            messages
            | "TimestampMessages" >> beam.ParDo(TimestampedValueDoFn())
            | "WriteMessages" >> self.message_sink
        )
        (
            segments
            | "TimestampSegments" >> beam.ParDo(TimestampedValueDoFn())
            | "WriteSegments" >> self.segment_sink(segmenter.segment_schema, 
                                                   self.options.seg_dest)
        )
        if self.options.legacy_seg_v1_dest:
            segments_v1 = segmented[segmenter.OUTPUT_TAG_SEGMENTS_V1]
            (
                segments_v1
                | "TimestampOldSegments" >> beam.ParDo(TimestampedValueDoFn())
                | "WriteOldSegments" >> self.segment_sink(segmenter.segment_schema_v1, 
                                                       self.options.legacy_seg_v1_dest)
            )
        return pipeline

    def run(self):
        return self.pipeline().run()


def run(options):

    pipeline = SegmentPipeline(options)
    result = pipeline.run()

    success_states = set([PipelineState.DONE])

    if pipeline.options.wait_for_job or options.view_as(StandardOptions).runner == 'DirectRunner':
        result.wait_until_finish()
    else:
        success_states.add(PipelineState.RUNNING)
        success_states.add(PipelineState.UNKNOWN)
        success_states.add(PipelineState.PENDING)

    logging.info('returning with result.state=%s' % result.state)
    return 0 if result.state in success_states else 1
