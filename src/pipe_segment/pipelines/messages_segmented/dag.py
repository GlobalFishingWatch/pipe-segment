"""Graph of the messages-segmented pipeline."""

import datetime as dt
import logging

from typing import Any, Callable

import apache_beam as beam
import ujson

from apache_beam.pvalue import PCollection

from gfw.common.beam.pipeline.dag import Dag
from gfw.common.beam.transforms import ReadFromBigQuery
from gfw.common.query import Query
from pipe_segment.pipelines.messages_segmented.config import MessagesSegmentedConfig
from pipe_segment.pipelines.messages_segmented.queries import (
    FragmentsQuery,
    MessagesQuery,
    PriorSegmentsQuery,
)
from pipe_segment.pipelines.messages_segmented.transforms import (
    SEGMAP,
    SEGMENTS,
    FilterBadSatelliteTimes,
    Fragment,
    FullHistorySegments,
    IncrementalSegments,
    SatelliteOffsets,
    TagWithFragIdAndTimeBin,
    TagWithSegId,
    WhitelistFields,
    filter_invalid_values,
)
from pipe_segment.tools import datetime_from_timestamp


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

        messages_sink:
            Transform that writes the segmented messages.

        segments_sink:
            Transform that writes the segments.

        fragments_sink:
            Transform that writes the fragments.

        satellite_offsets_sink:
            Transform that writes the satellite offsets. If None, they are not written.

        read_from_bigquery_factory:
            Builds the transform that reads from BigQuery.
    """

    def __init__(
        self,
        config: MessagesSegmentedConfig,
        messages_sink: beam.PTransform,
        segments_sink: beam.PTransform,
        fragments_sink: beam.PTransform,
        satellite_offsets_sink: beam.PTransform | None = None,
        read_from_bigquery_factory: Callable[..., Any] = beam.io.ReadFromBigQuery,
    ) -> None:
        """Initializes the graph."""
        self.config = config
        self.messages_sink = messages_sink
        self.segments_sink = segments_sink
        self.fragments_sink = fragments_sink
        self.satellite_offsets_sink = satellite_offsets_sink
        self.read_from_bigquery_factory = read_from_bigquery_factory

    @property
    def merge_params(self) -> dict:
        """Parameters of the segment matcher."""
        return ujson.loads(DEFAULT_MERGE_PARAMS)

    @property
    def segmenter_params(self) -> dict:
        """Parameters of the fragmenter."""
        return ujson.loads(DEFAULT_SEGMENT_PARAMS)

    def read(self, query: Query, label: str) -> beam.PTransform:
        """Returns the transform that reads a query from BigQuery, labeling its job."""
        return ReadFromBigQuery.from_query(
            query,
            label=label,
            read_from_bigquery_factory=self.read_from_bigquery_factory,
            read_from_bigquery_kwargs={"bigquery_job_labels": self.config.labels},
        )

    def segmenter(self, start_date: dt.date, last_day: dt.date) -> beam.PTransform:
        """Returns the transform that builds the segment map and the segments."""
        if self.config.incremental:
            prior_segments = PriorSegmentsQuery(
                segments_table=self.config.in_segments_table,
                fragments_table=self.config.in_fragments_table,
                day=start_date - dt.timedelta(days=1),
                ssvid_filter_query=self.config.ssvid_filter_query,
            )
            return IncrementalSegments(
                prior_segments=self.read(prior_segments, "ReadPriorSegments"),
                merge_params=self.merge_params,
            )

        # Without a start date, the query reads all the fragments.
        existing_fragments = FragmentsQuery(
            source=self.config.in_fragments_table,
            end_date=start_date - dt.timedelta(days=1),
        )
        return FullHistorySegments(
            self.read(existing_fragments, "ReadFragments"),
            start_date,
            last_day,
            self.merge_params,
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

        read_messages = self.read(
            MessagesQuery(
                source=config.bq_in_normalized_messages,
                start_date=start_date,
                end_date=last_day,
                ssvid_filter_query=config.ssvid_filter_query,
            ),
            "ReadMessages",
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
                read_from_bigquery_factory=self.read_from_bigquery_factory,
            )

            if self.satellite_offsets_sink is not None:
                logger.info("Adding WriteSatOffsets transform...")
                _ = satellite_offsets | "WriteSatOffsets" >> self.satellite_offsets_sink

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
