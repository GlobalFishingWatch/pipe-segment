from apache_beam.runners import PipelineState
from apache_beam.options.pipeline_options import GoogleCloudOptions
from apache_beam.options.pipeline_options import StandardOptions
from apache_beam.io.gcp.bigquery_tools import table_schema_to_dict

from apitools.base.py.exceptions import HttpError

from google.cloud import bigquery

from datetime import timedelta

from pipe_segment.utils.bqtools import BigQueryTools
from pipe_tools.timestamp import datetimeFromTimestamp
from pipe_tools.timestamp import timestampFromDatetime
from pipe_tools.utils.timestamp import as_timestamp

from pipe_segment.options.segment import SegmentOptions
from pipe_segment.transform.write_sink import WriteSink
from pipe_segment.transform.read_source import ReadSource
from pipe_segment.transform.read_messages_from_several_sources import ReadMessagesFromSeveralSources
from pipe_segment.transform.segment import Segment
from pipe_segment.transform.normalize import NormalizeDoFn

import apache_beam as beam
import logging
import ujson

def safe_dateFromTimestamp(ts):
    if ts is None:
        return None
    return datetimeFromTimestamp(ts).date()

def filter_by_ssvid_predicate(obj, ssvid_kkdict):
    return obj['ssvid'] in ssvid_kkdict

def build_sink_table_descriptor(table_id, schema, description):
    table = bigquery.Table(
        table_id.replace('bq://','').replace(':', '.'),
        schema=schema,
    )
    table.description = description
    return table

class FilterBySsvid(beam.PTransform):

    def __init__(self, ssvid_iter):
        self.ssvid_iter = ssvid_iter

    def expand(self, xs):
        ssvid = set(self.ssvid_iter)
        return (
            xs | beam.Filter(self.segment, lambda x: x['ssvid'] in ssvid)
        )


class SegmentPipeline:
    def __init__(self, options):
        self.options = options.view_as(SegmentOptions)
        range_as_timestamp = lambda s: list(map(as_timestamp, s.split(',')) if s is not None else (None, None))
        self.date_range_ts = range_as_timestamp(self.options.date_range)
        self._message_source_list = None
        self._read_from_several_sources = None

    @property
    def read_from_several_sources(self):
        if not self._read_from_several_sources:
            self._read_from_several_sources = ReadMessagesFromSeveralSources(self.options)
        return self._read_from_several_sources


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
        return WriteSink(
            self.options.msg_dest,
            table_schema_to_dict(self.read_from_several_sources.message_output_schema),
            "Daily satellite messages segmented processed in segment step."
        )

    @property
    def segment_source(self):
        if self.date_range_ts[0] is None:
            return beam.Create([])

        dt = datetimeFromTimestamp(self.date_range_ts[0])
        ts = timestampFromDatetime(dt - timedelta(days=1))

        try:
            source = ReadSource(self.options.seg_dest, ts, ts)
        except HttpError as exn:
            logging.warn(f"Previous day <{dt.isoformat()}> in Segment source <{self.options.seg_dest}> not found.")
            if exn.status_code == 404:
                return beam.Create([])
            else:
                raise
        return source

    def segment_sink(self, schema, table):
        sink = WriteSink(
            table,
            table_schema_to_dict(schema),
            "Daily segments processed in segment step."
        )
        return sink


    def pipeline(self):
        # Note that Beam appears to treat str(x) and unicode(x) as distinct
        # for purposes of CoGroupByKey, so both messages and segments should be
        # stringified or neither. 
        pipeline = beam.Pipeline(options=self.options)
        
        start_date=safe_dateFromTimestamp(self.date_range_ts[0])
        end_date=safe_dateFromTimestamp(self.date_range_ts[1])

        segmenter = Segment(start_date=start_date,
                            end_date=end_date,
                            segmenter_params=self.segmenter_params,
                            look_ahead=self.options.look_ahead)
        
        # Ensure sharded tables are created for all dates
        logging.info('Ensure sharded tables are created for all dates')

        destination_tables = [{"table": self.options.msg_dest,
                               "schema": self.read_from_several_sources.message_output_schema,
                               "description": "Daily satellite messages segmented processed in segment step.",
                               },
                              {"table": self.options.seg_dest,
                               "schema": segmenter.segment_schema,
                               "description": "Daily segments processed in segment step.",
                               },
                              ] + ([
                                  {"table": self.options.legacy_seg_v1_dest,
                                   "schema": segmenter.segment_schema_v1,
                                      "description": "Daily segments processed in segment step.",
                                   },
                              ] if self.options.legacy_seg_v1_dest else [])
        # Create list of days between start_date and end_date including the start_date.
        days = ([start_date]+[start_date+timedelta(days=x) for x in range((end_date-start_date).days)])
        for dt in days:
            shard = dt.strftime("%Y%m%d")

            for dst in destination_tables:
                sink_table_descriptor = build_sink_table_descriptor(table_id=f"{dst['table']}{shard}", 
                                                                    schema=table_schema_to_dict(dst['schema'])["fields"],
                                                                    description=dst['description'])
                bqtools = BigQueryTools(project=sink_table_descriptor.project)
                sink_table = bqtools.ensure_table_exists(sink_table_descriptor)
                bqtools.clear_records(sink_table, "timestamp", dt, dt)


        messages = (
            pipeline
            | self.read_from_several_sources
        )

        if self.options.ssvid_filter_query:
            target_ssvid = beam.pvalue.AsDict(
                messages
                | beam.io.ReadFromBigQuery(query=self.options.ssvid_filter_query, use_standard_sql=True)
                | beam.Map(lambda x: (x['ssvid'], x['ssvid']))
            )
            messages = (
                messages
                | beam.Filter(filter_by_ssvid_predicate, target_ssvid)
            )

        messages = (
            messages
            | "Normalize" >> beam.ParDo(NormalizeDoFn())
            | "MessagesAddKey" >> beam.Map(self.groupby_fn)
        )

        segments = (
            pipeline
            | "ReadLastDaySegments" >> self.segment_source
            | "RemoveClosedSegments" >> beam.Filter(lambda x: not x['closed'])
            | "SegmentsAddKey" >> beam.Map(self.groupby_fn)
        )

        args = (
            {'messages' : messages, 'segments' : segments}
            | 'GroupAllBySSVID' >> beam.CoGroupByKey()
        )

        # Runs the segmenter. Input: messages and segments group by ssvid {ssvid: {messages:m, segments:s}}
        segmented = args | "Segment" >> segmenter

        messages = segmented[segmenter.OUTPUT_TAG_MESSAGES]
        segments = segmented[segmenter.OUTPUT_TAG_SEGMENTS]
        (
            messages
            | "WriteMessages" >> self.message_sink
        )
        (
            segments
            | "WriteSegments" >> self.segment_sink(segmenter.segment_schema,
                                                   self.options.seg_dest)
        )
        if self.options.legacy_seg_v1_dest:
            segments_v1 = segmented[segmenter.OUTPUT_TAG_SEGMENTS_V1]
            (
                segments_v1
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
