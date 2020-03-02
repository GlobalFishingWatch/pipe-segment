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
from pipe_segment.transform.segment import Segment


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
    return list(map(as_timestamp, s.split(',')) if s is not None else (None, None))

def is_closed_in_date_range(seg, date_range):
    start, end = date_range
    return (seg['closed'] and start <= seg['timestamp'] <= end)


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
        sink = GCPSink(gcp_path=table,
                       schema=schema,
                       temp_gcs_location=self.temp_gcs_location,
                       temp_shards_per_day=self.options.temp_shards_per_day)
        return sink

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
    def segment_schema(self):
        return Segment().segment_schema
    

    @property
    def track_source(self):
        if self.date_range[0] is None:
            return beam.Create([])

        dt = datetimeFromTimestamp(self.date_range[0])
        ts = timestampFromDatetime(dt - timedelta(days=1))

        try:
            source = GCPSource(gcp_path=self.options.track_dest,
                             first_date_ts=ts,
                             last_date_ts=ts)
        except HttpError as exn:
            logging.warn("Tracks source not found: %s %s" % (self.options.track_dest, dt))
            if exn.status_code == 404:
                return beam.Create([])
            else:
                raise
        return source



    @property
    def segment_source_list(self):
        # creating a GCPSource requires calls to the bigquery API if we are
        # reading from bigquery, so only do this once.
        if not self._segment_source_list:
            first_date_ts, last_date_ts = self.date_range
            # TODO: first date should look back N-days and that should be passed to 
            # stitcher implementation as MAX_LOOKBACK
            last_date_ts = offset_timestamp(last_date_ts, days=self.options.look_ahead)
            # TODO: can we use offset_timestamp with negative days here?
            # dt = datetimeFromTimestamp(first_date_ts)
            # first_date_ts = timestampFromDatetime(dt - timedelta(days=1))

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

    @property
    def closed_segment_source(self):
        return GCPSource(gcp_path=self.options.closed_seg_table)

    @property
    def closed_segment_sink(self):
        assert self.options.closed_seg_table.startswith('bq://')
        table = self.options.closed_seg_table[5:]
        return beam.io.WriteToBigQuery(table=table, 
                                          schema=self.segment_schema,
                                          write_disposition='WRITE_APPEND')

    def pipeline(self):
        stitcher = Stitch(start_date=safe_dateFromTimestamp(self.date_range[0]),
                          end_date=safe_dateFromTimestamp(self.date_range[1]),
                          stitcher_params=self.stitcher_params,
                          look_ahead=self.options.look_ahead)

        pipeline = beam.Pipeline(options=self.options)
        track_sink = self.track_sink(stitcher.track_schema, 
                                     self.options.track_dest)
        black_list = set([x.strip() for x in self.options.black_list.split(',')])



        segments = (
            self.segment_sources(pipeline) 
            | "MergeSegments" >> beam.Flatten()
            | "SegmentsAddKey" >> beam.Map(self.groupby_fn)
            )

        tracks = (
            pipeline
            | "ReadTracks" >> self.track_source
            | "TracksAddKey" >> beam.Map(self.groupby_fn)
        )

        args = (
            {'segments' : segments, 'tracks' : tracks}
            | 'GroupByKey' >> beam.CoGroupByKey()
            | 'FilterBlacklist' >> beam.Filter(lambda x: x[0] not in black_list)
        )

        (
            args
            | "Stitch" >> stitcher
            | "TimestampTracks" >> beam.ParDo(TimestampedValueDoFn())
            | "WriteTracks" >> track_sink
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
