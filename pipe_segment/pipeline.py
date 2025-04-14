import apache_beam as beam
import datetime
import logging
import pytz
import ujson
from apache_beam.options.pipeline_options import (
    GoogleCloudOptions, StandardOptions, PipelineOptions
)

from apache_beam.runners import PipelineState

from datetime import timedelta

from pipe_segment import message_schema, segment_schema
from pipe_segment.models.bigquery_message_source import BigQueryMessagesSource
from pipe_segment.transform.create_segment_map import CreateSegmentMap
from pipe_segment.transform.create_segments import CreateSegments
from pipe_segment.transform.filter_bad_satellite_times import FilterBadSatelliteTimes
from pipe_segment.transform.fragment import Fragment
from pipe_segment.transform.invalid_values import filter_invalid_values
from pipe_segment.transform.read_fragments import ReadFragments
from pipe_segment.transform.read_messages import ReadMessages
from pipe_segment.transform.satellite_offsets import (
    SatelliteOffsets,
    get_description as satellite_offsets_description
)
from pipe_segment.transform.tag_with_fragid_and_timebin import TagWithFragIdAndTimeBin
from pipe_segment.transform.tag_with_seg_id import TagWithSegId
from pipe_segment.transform.write_sink import WriteSink
from pipe_segment.transform.whitelist_messages_segmented import WhitelistFields
from pipe_segment.utils.bqtools import BigQueryTools
from pipe_segment.version import __version__

from typing import List

from .tools import as_timestamp, datetimeFromTimestamp

logger = logging.getLogger(__name__)


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
    return x["ssvid"], x["frag_id"], str(dtime.date()), time_bin_ndx(dtime, time_bins)


def strip_identity_and_destination(fragment):
    x = fragment.copy()
    x["identities"] = []
    x["destinations"] = []
    return x


class SegmentPipeline:
    def __init__(self, options, beam_options):
        self.options = options
        self.beam_options = beam_options
        self.cloud_options = beam_options.view_as(GoogleCloudOptions)
        self.date_range = parse_date_range(self.options.date_range)
        self.bqtools = BigQueryTools(project=self.cloud_options.project)

        # Cleans each table content to start fresh
        for key in self.destination_tables:
            dtable = self.destination_tables[key]
            self.bqtools.remove_content(
                table=dtable["table"],
                date_range=self.options.date_range,
                labels=self.cloud_options.labels,
                partition_field=dtable["partition_field"],
            )

    @classmethod
    def build(cls, options, beam_args):
        beam_options = PipelineOptions(beam_args)

        return cls(options, beam_options)

    @property
    def merge_params(self):
        return ujson.loads(self.options.merge_params)

    @property
    def segmenter_params(self):
        return ujson.loads(self.options.segmenter_params)

    @property
    def source_tables(self) -> List[BigQueryMessagesSource]:
        return [BigQueryMessagesSource(table_id=source, bqtools=self.bqtools)
                for source in self.options.in_normalized_messages_table.split(",")]

    @property
    def destination_tables(self):
        ver = __version__
        result = dict(
            messages={
                "table": self.options.out_segmented_messages_table,
                "schema": message_schema.message_output_schema,
                "description": f"""Created by the pipe-segment:{ver}.
                Daily satellite messages segmented processed in segment step.""",
                "partition_field": "timestamp",
            },
            segments={
                "table": self.options.out_segments_table,
                "schema": segment_schema.segment_schema,
                "description": f"""Created by the pipe-segment:{ver}.
                Daily segments processed in segment step.""",
                "partition_field": "timestamp",
            },
            fragments={
                "table": self.options.out_fragments_table or self.options.fragments_table,
                "schema": Fragment.schema,
                "description": f"""Created by the pipe-segment:{ver}.
                Daily fragments processed in segment step.""",
                "partition_field": "timestamp",
            })

        if self.options.out_sat_offsets_table:
            result["sat_offset"] = {
                "table": self.options.out_sat_offsets_table,
                "schema": SatelliteOffsets.schema,
                "description": satellite_offsets_description(self.options, ver),
                "partition_field": "hour",
            }
        return result

    def write(self, tbl_data: dict) -> WriteSink:
        return WriteSink(
            tbl_data["table"],
            tbl_data["schema"],
            tbl_data["description"],
            tbl_data["partition_field"]
        )

    # TODO: consider breaking up
    def pipeline(self):
        logger.info("Creating pipeline object...")
        pipeline = beam.Pipeline(options=self.beam_options)

        start_date = safe_date(self.date_range[0])
        end_date = safe_date(self.date_range[1])

        logger.info("Adding ReadMessages transform...")
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
            self.options.in_normalized_sat_offset_messages_table
            and self.options.in_norad_to_receiver_table
            and self.options.in_sat_positions_table
        ):
            logger.info("Adding SatelliteOffsets transform...")

            satellite_offsets = pipeline | SatelliteOffsets(
                self.options.in_normalized_sat_offset_messages_table,
                self.options.in_norad_to_receiver_table,
                self.options.in_sat_positions_table,
                start_date,
                end_date,
            )

            if self.options.out_sat_offsets_table:
                (
                    satellite_offsets | self.write(self.destination_tables['sat_offset'])
                )

            logger.info("Adding FilterBadSatelliteTimes transform...")
            messages = messages | FilterBadSatelliteTimes(
                satellite_offsets,
                max_timing_offset_s=self.options.max_timing_offset_s,
                bad_hour_padding=self.options.bad_hour_padding,
            )

        logger.info("Adding MessagesAddKey and GroupBySsvidAndDay transforms...")
        messages = (
            messages
            | "MessagesAddKey"
            >> beam.Map(lambda x: ((x["ssvid"], str(safe_date(x["timestamp"]))), x))
            | "GroupBySsvidAndDay" >> beam.GroupByKey()
        )

        logger.info("Adding Fragment transform...")
        fragmented = messages | "Fragment" >> Fragment(
            fragmenter_params=self.segmenter_params
        )

        messages = fragmented[Fragment.OUTPUT_TAG_MESSAGES]
        new_fragments = fragmented[Fragment.OUTPUT_TAG_FRAGMENTS]

        logger.info("Adding WriteFragments transform...")

        new_fragments | "WriteFragments" >> self.write(self.destination_tables["fragments"])

        logger.info("Adding ReadFragments transform...")
        existing_fragments = pipeline | ReadFragments(
            self.options.fragments_table,
            project=self.cloud_options.project,
            # TODO should be able to use single lookback, but would have to
            # lookback at fragments not segments or something otherwise complicated
            start_date=None,  # start_date - timedelta(days=1),
            end_date=start_date - timedelta(days=1),
            labels=self.cloud_options.labels,
        )

        all_fragments = (new_fragments, existing_fragments) | beam.Flatten()

        segmap_src = (
            all_fragments
            | "StripIdentity" >> beam.Map(strip_identity_and_destination)
            | "AddSsvidKey" >> beam.Map(lambda x: (x["ssvid"], x))
            | "GroupBySsvid" >> beam.GroupByKey()
            | CreateSegmentMap(self.merge_params)
        )

        logger.info("Adding TagWithFragIdAndTimeBin transform...")
        bins_per_day = self.options.bins_per_day
        msg_segmap = segmap_src | TagWithFragIdAndTimeBin(
            start_date, end_date, bins_per_day
        )

        logger.info("Adding AddKeyToMessages transform...")
        tagged_messages = messages | "AddKeyToMessages" >> beam.Map(
            lambda x: (time_bin_key(x, bins_per_day), x)
        )

        logger.info(
            "Adding GroupMsgsWithMap, TagMsgsWithSegId, "
            "WhitelistFields, WriteMessages transforms..."
        )

        (
            {"segmap": msg_segmap, "target": tagged_messages}
            | "GroupMsgsWithMap" >> beam.CoGroupByKey()
            | "TagMsgsWithSegId" >> TagWithSegId()
            | "WhitelistFields" >> WhitelistFields()
            | "WriteMessages" >> self.write(self.destination_tables["messages"])
        )

        logger.info("Adding AddFragidKey transform...")
        frag_segmap = segmap_src | "AddFragidKey" >> beam.Map(
            lambda x: (x["frag_id"], x)
        )

        logger.info("Adding AddKeyToFragments transform...")
        tagged_fragments = all_fragments | "AddKeyToFragments" >> beam.Map(
            lambda x: (x["frag_id"], x)
        )

        logger.info("Adding WriteSegments transform...")
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
            | "WriteSegments" >> self.write(self.destination_tables["segments"])
        )

        return pipeline

    def run(self):
        logger.info("Building pipeline...")
        pipe = self.pipeline()

        logger.info("Running pipeline...")
        result = pipe.run()

        success_states = set([PipelineState.DONE])

        if (
            self.options.wait_for_job
            or self.beam_options.view_as(StandardOptions).runner == "DirectRunner"
        ):
            logger.info("Waiting until job is done")
            result.wait_until_finish()
            if result.state == PipelineState.DONE:
                for key in self.destination_tables:
                    dtable = self.destination_tables[key]
                    self.bqtools.update_description(dtable["table"], dtable["description"])
                    self.bqtools.update_labels(dtable["table"], self.cloud_options.labels)

        else:
            success_states.add(PipelineState.RUNNING)
            success_states.add(PipelineState.UNKNOWN)
            success_states.add(PipelineState.PENDING)

        logger.info("returning with result.state=%s" % result.state)

        return 0 if result.state in success_states else 1


def run(*args, **kwargs):
    return SegmentPipeline.build(*args, **kwargs).run()
