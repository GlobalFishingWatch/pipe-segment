"""Graph of the messages-segmented pipeline."""

import datetime as dt
import logging

import apache_beam as beam
import ujson

from apache_beam.pvalue import PCollection

from gfw.common.beam.pipeline.dag import Dag
from gfw.common.bigquery.helper import BigQueryHelper
from pipe_segment.pipelines.messages_segmented.config import MessagesSegmentedConfig
from pipe_segment.tools import datetime_from_timestamp
from pipe_segment.pipelines.messages_segmented.transforms.filter_bad_satellite_times import (
    FilterBadSatelliteTimes,
)
from pipe_segment.pipelines.messages_segmented.transforms.fragment import Fragment
from pipe_segment.pipelines.messages_segmented.transforms.full_history_segments import (
    SEGMAP,
    SEGMENTS,
    FullHistorySegments,
)
from pipe_segment.pipelines.messages_segmented.transforms.incremental_segments import (
    IncrementalSegments,
)
from pipe_segment.pipelines.messages_segmented.transforms.invalid_values import (
    filter_invalid_values,
)
from pipe_segment.pipelines.messages_segmented.transforms.read_messages import ReadMessages
from pipe_segment.pipelines.messages_segmented.transforms.read_prior_segments import (
    ReadPriorSegments,
)
from pipe_segment.pipelines.messages_segmented.transforms.satellite_offsets import (
    SatelliteOffsets,
    SatelliteOffsetsWrite,
)
from pipe_segment.pipelines.messages_segmented.transforms.tag_with_fragid_and_timebin import (
    TagWithFragIdAndTimeBin,
)
from pipe_segment.pipelines.messages_segmented.transforms.tag_with_seg_id import TagWithSegId
from pipe_segment.pipelines.messages_segmented.transforms.whitelist_messages_segmented import (
    WhitelistFields,
)


logger = logging.getLogger(__name__)

# These are default values used in our pipelines, tweak it to do research
DEFAULT_MERGE_PARAMS = "{}"
DEFAULT_SEGMENT_PARAMS = '{"max_hours": 24}'


def safe_date(ts: float | None) -> dt.date | None:
    """Returns the UTC date of a timestamp in seconds, or None if there is none."""
    if ts is None:
        return None

    return datetime_from_timestamp(ts).date()


def time_bin_ndx(dtime: dt.datetime, time_bins: int) -> int:
    """Returns the index of the bin of the day that a time falls in."""
    reltime = dtime - dt.datetime(dtime.year, dtime.month, dtime.day, tzinfo=dt.timezone.utc)
    ndx = int(time_bins * (reltime / dt.timedelta(hours=24)))
    assert 0 <= ndx < time_bins
    return ndx


def time_bin_key(x: dict, time_bins: int) -> tuple:
    """Returns the key that tags a message with its ssvid, fragment, day and bin of the day."""
    dtime = datetime_from_timestamp(x["timestamp"])
    return x["ssvid"], x["frag_id"], str(dtime.date()), time_bin_ndx(dtime, time_bins)


class MessagesSegmentedDag(Dag):
    """Reads the messages, fragments and segments them, and writes the three tables.

    Args:
        config:
            The pipeline's configuration.

        bq_helper:
            Helper that the transforms use to look up the tables they read.

        messages_sink:
            Transform that writes the segmented messages.

        segments_sink:
            Transform that writes the segments.

        fragments_sink:
            Transform that writes the fragments.
    """

    def __init__(
        self,
        config: MessagesSegmentedConfig,
        bq_helper: BigQueryHelper,
        messages_sink: beam.PTransform,
        segments_sink: beam.PTransform,
        fragments_sink: beam.PTransform,
    ) -> None:
        """Initializes the graph."""
        self.config = config
        self.bq_helper = bq_helper
        self.messages_sink = messages_sink
        self.segments_sink = segments_sink
        self.fragments_sink = fragments_sink

    @property
    def merge_params(self) -> dict:
        """Parameters of the segment matcher."""
        return ujson.loads(DEFAULT_MERGE_PARAMS)

    @property
    def segmenter_params(self) -> dict:
        """Parameters of the fragmenter."""
        return ujson.loads(DEFAULT_SEGMENT_PARAMS)

    def segmenter(self, start_date: dt.date, last_day: dt.date) -> beam.PTransform:
        """Returns the transform that builds the segment map and the segments."""
        if self.config.incremental:
            return IncrementalSegments(
                prior_segments=ReadPriorSegments(
                    self.config.in_segments_table,
                    self.config.in_fragments_table,
                    start_date - dt.timedelta(days=1),
                    ssvid_filter_query=self.config.ssvid_filter_query,
                    read_from_bigquery_kwargs={"bigquery_job_labels": self.config.labels},
                ),
                merge_params=self.merge_params,
            )

        return FullHistorySegments(
            self.config.in_fragments_table,
            start_date,
            last_day,
            self.merge_params,
            self.config.labels,
        )

    def apply(self, pipeline: beam.Pipeline) -> PCollection:
        """Applies the transforms to the pipeline.

        Returns:
            The written segmented messages.
        """
        config = self.config
        start_date = config.start_date
        # The end date of the config is exclusive, but the transforms work with the last day.
        last_day = config.end_date - dt.timedelta(days=1)

        read_messages = ReadMessages(
            bq_helper=self.bq_helper,
            sources=[config.bq_in_normalized_messages],
            start_date=start_date,
            end_date=last_day,
            labels=config.labels,
            ssvid_filter_query=config.ssvid_filter_query,
        )
        segmenter = self.segmenter(start_date, last_day)

        logger.info("Adding ReadMessages transform...")
        messages = (
            pipeline | read_messages | "FilterInvalidValues" >> beam.Map(filter_invalid_values)
        )

        if config.sat_offsets_enabled:
            logger.info("Adding SatelliteOffsets transform...")

            satellite_offsets = pipeline | SatelliteOffsets(
                config.bq_in_sat_offset_messages,
                config.bq_in_norad_to_receiver,
                config.bq_in_sat_positions,
                start_date,
                last_day,
                config.labels,
            )

            if config.bq_out_sat_offsets:
                logger.info("Adding SatelliteOffsetsWrite transform...")
                _ = satellite_offsets | SatelliteOffsetsWrite(config)

            logger.info("Adding FilterBadSatelliteTimes transform...")
            messages = messages | FilterBadSatelliteTimes(
                satellite_offsets,
                max_timing_offset_s=config.max_timing_offset_s,
                bad_hour_padding=config.bad_hour_padding,
            )

        logger.info("Adding MessagesAddKey and GroupBySsvidAndDay transforms...")
        messages = (
            messages
            | "MessagesAddKey"
            >> beam.Map(lambda x: ((x["ssvid"], str(safe_date(x["timestamp"]))), x))
            | "GroupBySsvidAndDay" >> beam.GroupByKey()
        )

        logger.info("Adding Fragment transform...")
        fragmented = messages | "Fragment" >> Fragment(fragmenter_params=self.segmenter_params)

        messages = fragmented[Fragment.OUTPUT_TAG_MESSAGES]
        new_fragments = fragmented[Fragment.OUTPUT_TAG_FRAGMENTS]

        logger.info("Adding WriteFragments transform...")
        _ = new_fragments | "WriteFragments" >> self.fragments_sink

        logger.info("Adding Segmenter transform...")
        segmented = new_fragments | "Segmenter" >> segmenter
        segmap_src = segmented[SEGMAP]

        logger.info("Adding TagWithFragIdAndTimeBin transform...")
        bins_per_day = config.bins_per_day
        msg_segmap = segmap_src | TagWithFragIdAndTimeBin(start_date, last_day, bins_per_day)

        logger.info("Adding AddKeyToMessages transform...")
        tagged_messages = messages | "AddKeyToMessages" >> beam.Map(
            lambda x: (time_bin_key(x, bins_per_day), x)
        )

        logger.info(
            "Adding GroupMsgsWithMap, TagMsgsWithSegId, WhitelistFields, WriteMessages "
            "transforms..."
        )
        written_messages = (
            {"segmap": msg_segmap, "target": tagged_messages}
            | "GroupMsgsWithMap" >> beam.CoGroupByKey()
            | "TagMsgsWithSegId" >> TagWithSegId()
            | "WhitelistFields" >> WhitelistFields()
            | "WriteMessages" >> self.messages_sink
        )

        logger.info("Adding WriteSegments transform...")
        _ = segmented[SEGMENTS] | "WriteSegments" >> self.segments_sink

        return written_messages
