import apache_beam as beam
import logging
import pytz
import ujson

from apache_beam.io.gcp.bigquery_tools import table_schema_to_dict
from apache_beam.options.pipeline_options import (GoogleCloudOptions, StandardOptions)
from apache_beam.runners import PipelineState

from apitools.base.py.exceptions import HttpError

from datetime import timedelta

from google.cloud import bigquery

from pipe_segment import message_schema, segment_schema
from pipe_segment.options.segment import SegmentOptions
from pipe_segment.transform.create_segment_map import CreateSegmentMap
from pipe_segment.transform.create_segments import CreateSegments
from pipe_segment.transform.filter_bad_satellite_times import FilterBadSatelliteTimes
from pipe_segment.transform.fragment import Fragment
from pipe_segment.transform.invalid_values import filter_invalid_values
from pipe_segment.transform.read_fragments import ReadFragments
from pipe_segment.transform.read_messages import ReadMessages
from pipe_segment.transform.read_messages_from_several_sources import ReadMessagesFromSeveralSources
from pipe_segment.transform.read_source import ReadSource
from pipe_segment.transform.satellite_offsets import SatelliteOffsets, SatelliteOffsetsWrite
from pipe_segment.transform.segment import Segment
from pipe_segment.transform.tag_with_fragid_and_timebin import TagWithFragIdAndTimeBin
from pipe_segment.transform.tag_with_seg_id import TagWithSegId
from pipe_segment.transform.write_date_sharded import WriteDateSharded
from pipe_segment.transform.write_sink import WriteSink
from pipe_segment.transform.whitelist_messages_segmented import WhitelistFields
from pipe_segment.utils.bqtools import BigQueryTools

from .tools import as_timestamp, datetimeFromTimestamp, timestampFromDatetime


def timestamp_to_date(ts: float) -> datetime.date:
    return datetimeFromTimestamp(ts).date()


def safe_date(ts):
    if ts is None:
        return None
    return datetimeFromTimestamp(ts).date()


def parse_date_range(s):
    # parse a string YYYY-MM-DD,YYYY-MM-DD into 2 timestamps
    return list(map(as_timestamp, s.split(",")) if s is not None else (None, None))


def time_bin_ndx(dtime, time_bins):
    reltime = dtime - datetime.datetime(dtime.year, dtime.month, dtime.day, tzinfo=pytz.UTC)
    ndx = int(time_bins * (reltime / timedelta(hours=24)))
    assert 0 <= ndx < time_bins
    return ndx


def time_bin_key(x, time_bins):
    dtime = datetimeFromTimestamp(x["timestamp"])
    return x["frag_id"], str(dtime.date()), time_bin_ndx(dtime, time_bins)


class SegmentPipeline:
    def __init__(self, options):
        self.cloud_options = options.view_as(GoogleCloudOptions)
        self.options = options.view_as(SegmentOptions)
        self.date_range = parse_date_range(self.options.date_range)
        self.satellite_offsets_writer = None

    @property
    def merge_params(self):
        return ujson.loads(self.options.merge_params)

    @property
    def segmenter_params(self):
        return ujson.loads(self.options.segmenter_params)

    @property
    def source_tables(self):
        return self.options.source.split(",")

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

    @property
    def message_sink(self):
        return WriteSink(
            destination_tables["messages"]["table"],
            destination_tables["messages"]["schema"],
            destination_tables["messages"]["description"]
        )

    @property
    def segment_sink(self):
        return WriteSink(
            destination_tables["segments"]["table"],
            destination_tables["segments"]["schema"],
            destination_tables["segments"]["description"]
        )

    @property
    def fragment_sink(self):
        return WriteSink(
            destination_tables["fragments"]["table"],
            destination_tables["fragments"]["schema"],
            destination_tables["fragments"]["description"]
        )

    @property
    def destination_tables(self):
        return dict(
            messages={
                "table": self.options.msg_dest,
                "schema": message_schema.message_output_schema,
                "description": "Daily satellite messages segmented processed in segment step.",
            },
            segments={
                "table": self.options.segment_dest,
                "schema": segment_schema.segment_schema,
                "description": "Daily segments processed in segment step.",
            },
            fragments={
                "table": self.options.fragment_tbl,
                "schema": Fragment.schema,
                "description": "Daily fragments processed in segment step.",
            }
        )

    def ensure_sharded_tables_creation(self):
        # Ensure sharded tables are created for all dates
        logging.info('Ensure sharded tables are created for all dates')

        # Create list of days between start_date and end_date including the start_date.
        days = ([start_date]+[start_date+timedelta(days=x) for x in range((end_date-start_date).days)])
        for dt in days:
            shard = dt.strftime("%Y%m%d")

            for dst in self.destination_tables.keys():
                sink_table_descriptor = build_sink_table_descriptor(
                    table_id=f"{self.destination_tables[dst]['table']}{shard}",
                    schema=table_schema_to_dict(self.destination_tables[dst]['schema'])["fields"],
                    description=self.destination_tables[dst]['description']
                )
                bqtools = BigQueryTools(project=sink_table_descriptor.project)
                sink_table = bqtools.ensure_table_exists(sink_table_descriptor)
                bqtools.clear_records(sink_table, "timestamp", dt, dt)

    # TODO: consider breaking up
    def pipeline(self):
        pipeline = beam.Pipeline(options=self.options)

        start_date=safe_date(self.date_range_ts[0])
        end_date=safe_date(self.date_range_ts[1])

        self.ensure_sharded_tables_creation()

        messages = (
            pipeline
            | ReadMessages(
                sources=self.source_tables,
                start_date=start_date,
                end_date=end_date,
                ssvid_filter_query=self.options.ssvid_filter_query,
            )
            | "FilterInvalidValues" >> beam.Map(filter_invalid_values)
        )

        if (
            self.options.sat_source
            and self.options.norad_to_receiver_tbl
            and self.options.sat_positions_tbl
        ):
            satellite_offsets = pipeline | SatelliteOffsets(
                self.options.sat_source,
                self.options.norad_to_receiver_tbl,
                self.options.sat_positions_tbl,
                start_date,
                end_date,
            )

            if self.options.sat_offset_dest:
                self.satellite_offsets_writer = SatelliteOffsetsWrite(self.options, self.cloud_options)
                (
                    satellite_offsets | self.satellite_offsets_writer
                )

            messages = messages | FilterBadSatelliteTimes(
                satellite_offsets,
                max_timing_offset_s=self.options.max_timing_offset_s,
                bad_hour_padding=self.options.bad_hour_padding,
            )

        messages = (
            messages
            | "MessagesAddKey"
            >> beam.Map(lambda x: ((x["ssvid"], str(safe_date(x["timestamp"]))), x))
            | "GroupBySsvidAndDay" >> beam.GroupByKey()
        )

        fragmented = messages | "Fragment" >> Fragment(
            fragmenter_params=self.segmenter_params
        )

        messages = fragmented[Fragment.OUTPUT_TAG_MESSAGES]
        new_fragments = fragmented[Fragment.OUTPUT_TAG_FRAGMENTS]

        new_fragments | self.fragment_sink

        existing_fragments = pipeline | ReadFragments(
            self.options.fragment_tbl,
            project=self.cloud_options.project,
            # TODO should be able to use single lookback, but would have to
            # lookback at fragments not segments or something otherwise complicated
            start_date=None,  # start_date - timedelta(days=1),
            end_date=start_date - timedelta(days=1),
            create_if_missing=True,
        )

        all_fragments = (new_fragments, existing_fragments) | beam.Flatten()

        segmap_src = (
            all_fragments
            | "AddSsvidKey" >> beam.Map(lambda x: (x["ssvid"], x))
            | "GroupBySsvid" >> beam.GroupByKey()
            | CreateSegmentMap(self.merge_params)
        )

        bins_per_day = self.options.bins_per_day
        msg_segmap = segmap_src | TagWithFragIdAndTimeBin(
            start_date, end_date, bins_per_day
        )

        tagged_messages = messages | "AddKeyToMessages" >> beam.Map(
            lambda x: (time_bin_key(x, bins_per_day), x)
        )

        (
            {"segmap": msg_segmap, "target": tagged_messages}
            | "GroupMsgsWithMap" >> beam.CoGroupByKey()
            | "TagMsgsWithSegId" >> TagWithSegId()
            | "WhitelistFields" >> WhitelistFields()
            | "WriteMessages" >> self.message_sink
        )

        frag_segmap = segmap_src | "AddFragidKey" >> beam.Map(
            lambda x: (x["frag_id"], x)
        )

        tagged_fragments = all_fragments | "AddKeyToFragments" >> beam.Map(
            lambda x: (x["frag_id"], x)
        )

        (
            {"segmap": frag_segmap, "target": tagged_fragments}
            | "GroupSegsWithMap" >> beam.CoGroupByKey()
            | "TagSegsWithsSegId" >> TagWithSegId()
            | "AddSegidKey" >> beam.Map(lambda x: (x["seg_id"], x))
            | "GroupBySegId" >> beam.GroupByKey()
            | "AddCumulativeData" >> CreateSegments()
            | "FilterSegsToDateRange"
            >> beam.Filter(
                lambda x: start_date <= timestamp_to_date(x["timestamp"]) <= end_date
            )
            | "WriteSegments" >> self.segment_sink()
        )

        return pipeline

    def run(self):
        return self.pipeline().run()


def run(options):

    pipeline = SegmentPipeline(options)
    result = pipeline.run()

    success_states = set([PipelineState.DONE])

    if (
        pipeline.options.wait_for_job
        or options.view_as(StandardOptions).runner == "DirectRunner"
    ):
        result.wait_until_finish()
        if (result.state == PipelineState.DONE and pipeline.satellite_offsets_writer):
            pipeline.satellite_offsets_writer.update_table_description()
            pipeline.satellite_offsets_writer.update_labels()

    else:
        success_states.add(PipelineState.RUNNING)
        success_states.add(PipelineState.UNKNOWN)
        success_states.add(PipelineState.PENDING)

    logging.info("returning with result.state=%s" % result.state)
    return 0 if result.state in success_states else 1
