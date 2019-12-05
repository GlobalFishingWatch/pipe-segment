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

from pipe_segment.options.stitcher import StitcherOptions
from pipe_segment.transform.stitcher import Stitch
from pipe_segment.transform.normalize import NormalizeDoFn
from pipe_segment.io.gcp import GCPSource
from pipe_segment.io.gcp import GCPSink


def safe_dateFromTimestamp(ts):
    if ts is None:
        return None
    return datetimeFromTimestamp(ts).date()

def offset_timestamp(ts, **timedelta_args):
    if ts is None:
        return ts
    dt = datetimeFromTimestamp(ts) + timedelta(**timedelta_args)
    return timestampFromDatetime(dt)

def parse_date_range(s):
    # parse a string YYYY-MM-DD,YYYY-MM-DD into 2 timestamps
    return map(as_timestamp, s.split(',')) if s is not None else (None, None)


class StitcherPipeline:
    def __init__(self, options):
        self.options = options.view_as(StitcherOptions)
        self._segment_source_list = None
        self.date_range = parse_date_range(self.options.date_range)

    @property
    def stitcher_params(self):
        return ujson.loads(self.options.stitcher_params)

    @property
    def temp_gcs_location(self):
        return self.options.view_as(GoogleCloudOptions).temp_location

    @staticmethod
    def groupby_fn(msg):
        return (msg['ssvid'], msg)

    def track_sink(self, schema, table):
        return beam.io.WriteToBigQuery(
            table,
            schema=schema,
            write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED)

    @property
    def segment_source(self):
        try:
            source = GCPSource(gcp_path=self.options.seg_source,
                               first_date_ts=None,
                               last_date_ts=None)
        except HttpError as exn:
            logging.warn("Segment source not found: %s" % (self.options.seg_source))
            raise
        return source

    @property
    def segment_source_list(self):
        # creating a GCPSource requires calls to the bigquery API if we are
        # reading from bigquery, so only do this once.
        if not self._segment_source_list:
            first_date_ts, last_date_ts = self.date_range
            gcp_paths = self.options.seg_source.split(',')
            self._segment_source_list = []
            for gcp_path in gcp_paths:
                s = GCPSource(gcp_path=gcp_path,
                               first_date_ts=first_date_ts,
                               last_date_ts=last_date_ts)
                self._segment_source_list.append(s)

        return self._segment_source_list

    def segment_sources(self, pipeline):
        def compose(idx, source):
            return pipeline | "Source%i" % idx >> source

        return (compose (idx, source) for idx, source in enumerate(self.segment_source_list))

    def pipeline(self):
        stitcher = Stitch(stitcher_params=self.stitcher_params)

        pipeline = beam.Pipeline(options=self.options)
        track_sink = self.track_sink(stitcher.track_schema, 
                                     self.options.track_dest)

        (
            self.segment_sources(pipeline)
            | "MergeSegments" >> beam.Flatten()
            | "SegmentsAddKey" >> beam.Map(self.groupby_fn)
            | 'GroupByKey' >> beam.GroupByKey()
            | "Stitch" >> stitcher
            | "WriteMessages" >> track_sink
        )

        return pipeline

    def run(self):
        return self.pipeline().run()


def run(options):

    pipeline = StitcherPipeline(options)
    result = pipeline.run()

    success_states = set([PipelineState.DONE])

    if pipeline.options.wait_for_job or options.view_as(StandardOptions).runner == 'DirectRunner':
        result.wait_until_finish()
    else:
        success_states.add(PipelineState.RUNNING)
        success_states.add(PipelineState.UNKNOWN)

    logging.info('returning with result.state=%s' % result.state)
    return 0 if result.state in success_states else 1
