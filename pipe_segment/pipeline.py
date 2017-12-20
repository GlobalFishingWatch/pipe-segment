import logging
import ujson
from datetime import timedelta
from apitools.base.py.exceptions import HttpError

import apache_beam as beam
from apache_beam.runners import PipelineState
from apache_beam.io.gcp.internal.clients.bigquery import TableFieldSchema
from apache_beam.options.pipeline_options import GoogleCloudOptions

from pipe_tools.timestamp import TimestampedValueDoFn
from pipe_tools.timestamp import datetimeFromTimestamp
from pipe_tools.timestamp import timestampFromDatetime
from pipe_tools.utils.timestamp import as_timestamp
from pipe_tools.io.bigquery import parse_table_schema

from pipe_segment.options import SegmentOptions
from pipe_segment.transform import Segment
from pipe_segment.io.gcp import GCPSource
from pipe_segment.io.gcp import GCPSink


def parse_date_range(s):
    # parse a string YYYY-MM-DD,YYYY-MM-DD into 2 timestamps
    return map(as_timestamp, s.split(',')) if s is not None else (None, None)


class SegmentPipeline:
    def __init__(self, options):
        self.options = options.view_as(SegmentOptions)
        self.date_range = parse_date_range(self.options.date_range)
        d1, d2 = self.date_range
        self.message_source = GCPSource(gcp_path=self.options.source,
                           first_date_ts=d1,
                           last_date_ts=d2)
        self.segment_transform = Segment(self.segmenter_params)

    @property
    def message_input_schema(self):
        schema = self.message_source.schema or self.options.source_schema
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
        field.type = "STRING"
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
        sink = GCPSink(gcp_path=self.options.dest,
                       schema=self.message_output_schema,
                       temp_gcs_location=self.temp_gcs_location)
        return sink

    @property
    def segment_source(self):
        if self.date_range[0] is None:
            return None

        dt = datetimeFromTimestamp(self.date_range[0])
        ts = timestampFromDatetime(dt - timedelta(days=1))

        try:
            sink = GCPSource(gcp_path=self.options.segments,
                             first_date_ts=ts,
                             last_date_ts=ts)
        except HttpError as exn:
            logging.warn("Segment source not found: %s %s" % (self.options.segments, dt))
            if exn.status_code == 404:
                return None
            else:
                raise
        return sink

    @property
    def segment_sink(self):
        sink = GCPSink(gcp_path=self.options.segments,
                       schema=self.segment_transform.segment_schema,
                       temp_gcs_location=self.temp_gcs_location)
        return sink


    def pipeline(self):
        pipeline = beam.Pipeline(options=self.options)
        messages = (
            pipeline
            | "ReadMessages" >> self.message_source
            | "MessagesAddKey" >> beam.Map(self.groupby_fn)
        )

        segment_source = self.segment_source
        if segment_source is None:
            segments_in = []
        else:
            segments_in = (
                pipeline
                | "ReadSegments" >> self.segment_source
                | "SegmentsAddKey" >> beam.Map(self.groupby_fn)
            )

        segmented = (
            {"messages": messages, "segments":segments_in}
            | beam.CoGroupByKey()
            | "Segment" >> self.segment_transform
        )
        messages = segmented[Segment.OUTPUT_TAG_MESSAGES]
        segments = segmented[Segment.OUTPUT_TAG_SEGMENTS]
        (
            messages
            | "TimestampMessages" >> beam.ParDo(TimestampedValueDoFn())
            | "WriteMessages" >> self.message_sink
        )
        (
            segments
            | "TimestampSegments" >> beam.ParDo(TimestampedValueDoFn())
            | "WriteSegments" >> self.segment_sink
        )
        return pipeline

    def run(self):
        return self.pipeline().run()


def run(options):

    pipeline = SegmentPipeline(options)
    result = pipeline.run()

    success_states = set([PipelineState.DONE])

    if pipeline.options.wait:
        result.wait_until_finish()
    else:
        success_states.add(PipelineState.RUNNING)
        success_states.add(PipelineState.UNKNOWN)

    logging.info('returning with result.state=%s' % result.state)
    return 0 if result.state in success_states else 1
