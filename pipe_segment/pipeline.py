import logging
from datetime import datetime, timedelta

import apache_beam as beam
import ujson
from apache_beam.options.pipeline_options import (GoogleCloudOptions,
                                                  StandardOptions)
from apache_beam.runners import PipelineState
from pipe_segment import message_schema
from pipe_segment.options.segment import SegmentOptions
from pipe_segment.transform.add_cumulative_data import AddCumulativeData
from pipe_segment.transform.create_segments import CreateSegments
from pipe_segment.transform.filter_bad_satellite_times import \
    FilterBadSatelliteTimes
from pipe_segment.transform.fragment import Fragment
from pipe_segment.transform.invalid_values import filter_invalid_values
from pipe_segment.transform.read_fragments import ReadFragments
from pipe_segment.transform.read_messages import ReadMessages
from pipe_segment.transform.satellite_offsets import SatelliteOffsets
from pipe_segment.transform.tag_with_fragid_and_date import \
    TagWithFragIdAndDate
from pipe_segment.transform.tag_with_seg_id import TagWithSegId
from pipe_segment.transform.write_date_sharded import WriteDateSharded

from .tools import as_timestamp, datetimeFromTimestamp


def timestamp_to_date(ts: float) -> datetime.date:
    return datetimeFromTimestamp(ts).date()


def safe_date(ts):
    if ts is None:
        return None
    return datetimeFromTimestamp(ts).date()


def parse_date_range(s):
    # parse a string YYYY-MM-DD,YYYY-MM-DD into 2 timestamps
    return list(map(as_timestamp, s.split(",")) if s is not None else (None, None))


class SegmentPipeline:
    def __init__(self, options):
        self.cloud_options = options.view_as(GoogleCloudOptions)
        self.options = options.view_as(SegmentOptions)
        self.date_range = parse_date_range(self.options.date_range)

    @property
    def merge_params(self):
        return ujson.loads(self.options.merge_params)

    @property
    def segmenter_params(self):
        return ujson.loads(self.options.segmenter_params)

    @property
    def source_tables(self):
        return self.options.source.split(",")

    # TODO: consider breaking up
    def pipeline(self):
        pipeline = beam.Pipeline(options=self.options)

        start_date = safe_date(self.date_range[0])
        end_date = safe_date(self.date_range[1])

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

        if self.options.sat_source:
            satellite_offsets = pipeline | SatelliteOffsets(
                self.options.sat_source, start_date, end_date
            )

            if self.options.sat_offset_dest:
                (
                    satellite_offsets
                    | "WriteSatOffsets"
                    >> WriteDateSharded(
                        self.options.sat_offset_dest,
                        self.cloud_options.project,
                        SatelliteOffsets.schema,
                        key="hour",
                    )
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

        existing_fragments = pipeline | ReadFragments(
            self.options.segment_dest,
            project=self.cloud_options.project,
            start_date=None,
            end_date=start_date - timedelta(days=1),
            create_if_missing=True,
        )

        all_fragments = (new_fragments, existing_fragments) | beam.Flatten()

        segments = (
            all_fragments
            | "AddSsvidKey" >> beam.Map(lambda x: (x["ssvid"], x))
            | "GroupBySsvid" >> beam.GroupByKey()
            | CreateSegments(self.merge_params)
        )

        msg_segmap = segments | TagWithFragIdAndDate(start_date, end_date)

        frag_segmap = segments | "AddFragidKey" >> beam.Map(lambda x: (x["frag_id"], x))

        tagged_messages = messages | "AddKeyToMessages" >> beam.Map(
            lambda x: ((x["frag_id"], str(timestamp_to_date(x["timestamp"]))), x)
        )

        (
            {"segmap": msg_segmap, "target": tagged_messages}
            | "GroupMsgsWithMap" >> beam.CoGroupByKey()
            | "TagMsgsWithSegId" >> TagWithSegId()
            | "WriteMessages"
            >> WriteDateSharded(
                self.options.msg_dest,
                self.cloud_options.project,
                message_schema.message_output_schema,
            )
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
            | "AddCumulativeData" >> AddCumulativeData()
            # | "FilterFragsToDateRange"
            # >> beam.Filter(
            #     lambda x: start_date <= timestamp_to_date(x["timestamp"]) <= end_date
            # )
            | "WriteFragments"
            >> WriteDateSharded(
                self.options.segment_dest,
                self.cloud_options.project,
                Fragment.schema,
            )
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
    else:
        success_states.add(PipelineState.RUNNING)
        success_states.add(PipelineState.UNKNOWN)
        success_states.add(PipelineState.PENDING)

    logging.info("returning with result.state=%s" % result.state)
    return 0 if result.state in success_states else 1
