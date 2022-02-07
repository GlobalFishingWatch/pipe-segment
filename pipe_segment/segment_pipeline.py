import logging
import ujson
from datetime import datetime, timedelta

import apache_beam as beam
from apache_beam.runners import PipelineState
from apache_beam.options.pipeline_options import GoogleCloudOptions
from apache_beam.options.pipeline_options import StandardOptions

from .timestamp import datetimeFromTimestamp
from .timestamp import as_timestamp

from pipe_segment.options.segment import SegmentOptions
from pipe_segment.transform.invalid_values import filter_invalid_values
from pipe_segment.transform.fragment import Fragment
from pipe_segment.transform.satellite_offsets import SatelliteOffsets

from pipe_segment.transform.filter_bad_satellite_times import FilterBadSatelliteTimes
from pipe_segment.transform.read_messages import ReadMessages
from pipe_segment.transform.read_fragments import ReadFragments
from pipe_segment.transform.create_segments import CreateSegments
from pipe_segment.transform.tag_with_seg_id import TagWithSegId
from pipe_segment.transform.write_date_sharded import WriteDateSharded
from pipe_segment import message_schema


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
        self._message_source_list = None

    # TODO: add to SatOffsets
    @property
    def sat_offset_schema(self):
        schema = {"fields": []}

        def add_field(name, field_type, mode="REQUIRED"):
            schema["fields"].append(
                dict(
                    name=name,
                    type=field_type,
                    mode=mode,
                )
            )

        add_field("hour", "timestamp")
        add_field("receiver", "STRING")
        add_field("dt", "FLOAT")
        add_field("pings", "INTEGER")
        add_field("avg_distance_from_sat_km", "FLOAT")
        add_field("med_dist_from_sat_km", "FLOAT")

        return schema

    # Export with below
    @property
    def message_source_list(self):
        gcp_paths = self.options.source.split(",")
        start_date = safe_date(self.date_range[0])
        end_date = safe_date(self.date_range[1])
        self._message_source_list = []
        for gcp_path in gcp_paths:
            s = ReadMessages(
                source=gcp_path,
                start_date=start_date,
                end_date=end_date,
                ssvid_filter_query=self.options.ssvid_filter_query,
            )
            self._message_source_list.append(s)

        return self._message_source_list

    # TODO: export to a transform
    def message_sources(self, pipeline):
        def compose(idx, source):
            return pipeline | "Source%i" % idx >> source

        return (
            compose(idx, source) for idx, source in enumerate(self.message_source_list)
        )

    @staticmethod
    def groupby_fn(msg):
        return ((msg["ssvid"], safe_date(msg["timestamp"])), msg)

    @property
    def merge_params(self):
        return ujson.loads(self.options.merge_params)

    @property
    def segmenter_params(self):
        return ujson.loads(self.options.segmenter_params)

    def pipeline(self):
        pipeline = beam.Pipeline(options=self.options)

        start_date = safe_date(self.date_range[0])
        end_date = safe_date(self.date_range[1])

        messages = (
            self.message_sources(pipeline)
            | "MergeMessages" >> beam.Flatten()
            | "FilterInvalidValues" >> beam.Map(filter_invalid_values)
        )

        if self.options.sat_source:
            satellite_offsets = pipeline | SatelliteOffsets(start_date, end_date)

            if self.options.sat_offset_dest:
                (
                    satellite_offsets
                    | "WriteSatOffsets"
                    >> WriteDateSharded(
                        self.options.sat_offset_dest,
                        self.cloud_options.project,
                        self.sat_offset_schema,
                    )
                )

            messages = messages | FilterBadSatelliteTimes(
                satellite_offsets,
                max_timing_offset_s=self.options.max_timing_offset_s,
                bad_hour_padding=self.options.bad_hour_padding,
            )

        messages = (
            messages
            | "MessagesAddKey" >> beam.Map(self.groupby_fn)
            | "GroupBySsvidAndDay" >> beam.GroupByKey()
        )

        fragmenter = Fragment(
            # TODO: WRONG. Segmenter shouldn't take start and end date
            # it should only process current one day ever.
            start_date=safe_date(self.date_range[0]),
            end_date=safe_date(self.date_range[1]),
            fragmenter_params=self.segmenter_params,
        )

        fragmented = messages | "Fragment" >> fragmenter

        messages = fragmented[fragmenter.OUTPUT_TAG_MESSAGES]
        new_fragments = fragmented[fragmenter.OUTPUT_TAG_FRAGMENTS]

        long_ago = datetime(1900, 1, 1)

        existing_fragments = pipeline | ReadFragments(
            self.options.segment_dest,
            project=self.cloud_options.project,
            start_date=long_ago,
            end_date=start_date - timedelta(days=1),
            create_if_missing=True,
        )

        fragmap = (
            (new_fragments, existing_fragments)
            | beam.Flatten()
            | "AddSsvidKey" >> beam.Map(lambda x: (x["ssvid"], x))
            | "GroupBySsvid" >> beam.GroupByKey()
            | CreateSegments(self.merge_params)
            | "AddKeyToFragmap" >> beam.Map(self.add_frag_key)
        )

        tagged_messages = messages | "AddKeyToMessages" >> beam.Map(
            lambda x: (x["frag_id"], x)
        )

        (
            {"fragmap": fragmap, "target": tagged_messages}
            | "GroupMsgsWithMap" >> beam.CoGroupByKey()
            | "TagMsgsWithSegId" >> TagWithSegId()
            | "WriteMessages"
            >> WriteDateSharded(
                self.options.msg_dest,
                self.cloud_options.project,
                message_schema.message_output_schema,
            )
        )

        tagged_fragments = new_fragments | "AddKeyToFragments" >> beam.Map(
            lambda x: (x["frag_id"], x)
        )

        (
            {"fragmap": fragmap, "target": tagged_fragments}
            | "GroupSegsWithMap" >> beam.CoGroupByKey()
            | "TagSegsWithsSegId" >> TagWithSegId()
            | "WriteFragments"
            >> WriteDateSharded(
                self.options.segment_dest,
                self.cloud_options.project,
                fragmenter.fragment_schema,
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
