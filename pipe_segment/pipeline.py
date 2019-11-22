import logging
import ujson
from datetime import timedelta
from apitools.base.py.exceptions import HttpError

import apache_beam as beam
from apache_beam.runners import PipelineState
from apache_beam.io.gcp.internal.clients.bigquery import TableFieldSchema
from apache_beam.options.pipeline_options import GoogleCloudOptions
from apache_beam.options.pipeline_options import StandardOptions

from pipe_tools.timestamp import TimestampedValueDoFn
from pipe_tools.timestamp import datetimeFromTimestamp
from pipe_tools.timestamp import timestampFromDatetime
from pipe_tools.utils.timestamp import as_timestamp
from pipe_tools.io.bigquery import parse_table_schema

from pipe_segment.options import SegmentOptions
from pipe_segment.transform.segment import Segment
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
    return map(as_timestamp, s.split(',')) if s is not None else (None, None)

def offset_timestamp(ts, **timedelta_args):
    if ts is None:
        return ts
    dt = datetimeFromTimestamp(ts) + timedelta(**timedelta_args)
    return timestampFromDatetime(dt)

class SegmentPipeline:
    def __init__(self, options):
        self.options = options.view_as(SegmentOptions)
        self.date_range = parse_date_range(self.options.date_range)
        self._message_source_list = None

    @property
    def message_source_list(self):
        # creating a GCPSource requires calls to the bigquery API if we are
        # reading from bigquery, so only do this once.
        if not self._message_source_list:
            first_date_ts, last_date_ts = self.date_range
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
            logging.warn("Segment source not found: %s %s" % (self.options.old_seg_dest, dt))
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
        messages = self.message_sources(pipeline)
        messages = (
            messages
            | "MergeMessages" >> beam.Flatten()
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
        if self.options.old_seg_dest:
            segments_v1 = segmented[segmenter.OUTPUT_TAG_SEGMENTS_V1]
            (
                segments_v1
                | "TimestampOldSegments" >> beam.ParDo(TimestampedValueDoFn())
                | "WriteOldSegments" >> self.segment_sink(segmenter.segment_schema_v1, 
                                                       self.options.old_seg_dest)
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

    logging.info('returning with result.state=%s' % result.state)
    return 0 if result.state in success_states else 1
