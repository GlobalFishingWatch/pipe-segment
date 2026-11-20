import datetime as dt
import logging

from google.cloud import bigquery
import ujson

import apache_beam as beam
from apache_beam.options.pipeline_options import (
    GoogleCloudOptions,
    PipelineOptions,
    StandardOptions,
)
from apache_beam.runners import PipelineState
from pipe_segment.hooks import verify_messages_exist_hook, verify_prior_segments_hook
from pipe_segment.schemas import message_schema, segment_schema
from pipe_segment.transform.filter_bad_satellite_times import FilterBadSatelliteTimes
from pipe_segment.transform.fragment import Fragment
from pipe_segment.transform.full_history_segments import (
    SEGMAP,
    SEGMENTS,
    FullHistorySegments,
)
from pipe_segment.transform.incremental_segments import IncrementalSegments
from pipe_segment.transform.invalid_values import filter_invalid_values
from pipe_segment.transform.read_messages import ReadMessages
from pipe_segment.transform.read_prior_segments import ReadPriorSegments
from pipe_segment.transform.satellite_offsets import (
    SatelliteOffsets,
    SatelliteOffsetsWrite,
)
from pipe_segment.transform.tag_with_fragid_and_timebin import TagWithFragIdAndTimeBin
from pipe_segment.transform.tag_with_seg_id import TagWithSegId
from pipe_segment.transform.whitelist_messages_segmented import WhitelistFields
from pipe_segment.utils.bq_tools import BigQueryHelper, DatePartitionedTable
from pipe_segment.version import __version__

from .tools import datetime_from_timestamp, timestamp_from_string

logger = logging.getLogger(__name__)

DESCRIPTION_TABLE_MESSAGES = f"""
Created by the pipe-segment:{__version__}.
Daily satellite messages segmented processed in segment step."""
DESCRIPTION_TABLE_SEGMENTS = f"""
Created by the pipe-segment:{__version__}.
Daily segments processed in segment step."""
DESCRIPTION_TABLE_FRAGMENTS = f"""Created by the pipe-segment:{__version__}.
Daily fragments processed in segment step."""
# These are default values used in our pipelines, tweak it to do research
DEFAULT_MERGE_PARAMS = "{}"
DEFAULT_SEGMENT_PARAMS = "{\"max_hours\": 24}"


def safe_date(ts):
    if ts is None:
        return None
    return datetime_from_timestamp(ts).date()


def parse_date_range(s):
    # parse a string YYYY-MM-DD,YYYY-MM-DD into 2 timestamps
    return list(map(timestamp_from_string, s.split(",")) if s is not None else (None, None))


def time_bin_ndx(dtime, time_bins):
    reltime = dtime - dt.datetime(dtime.year, dtime.month, dtime.day, tzinfo=dt.timezone.utc)
    ndx = int(time_bins * (reltime / dt.timedelta(hours=24)))
    assert 0 <= ndx < time_bins
    return ndx


def time_bin_key(x, time_bins):
    dtime = datetime_from_timestamp(x["timestamp"])
    return x["ssvid"], x["frag_id"], str(dtime.date()), time_bin_ndx(dtime, time_bins)


class SegmentPipeline:
    def __init__(self, options, beam_options):
        self.options = options
        self.beam_options = beam_options
        self.cloud_options = beam_options.view_as(GoogleCloudOptions)
        assert self.cloud_options.project is not None
        self.date_range = parse_date_range(self.options.date_range)
        self.bq_helper = BigQueryHelper(
            bq_client=bigquery.Client(project=self.cloud_options.project),
            labels=self.cloud_options.labels,
        )

    @classmethod
    def build(cls, options, beam_args):
        beam_options = PipelineOptions(beam_args)

        return cls(options, beam_options)

    @property
    def merge_params(self):
        return ujson.loads(DEFAULT_MERGE_PARAMS)

    @property
    def segmenter_params(self):
        return ujson.loads(DEFAULT_SEGMENT_PARAMS)

    @property
    def source_tables(self) -> list:
        return self.options.in_normalized_messages_table.split(",")

    @property
    def messages_output_table(self) -> DatePartitionedTable:
        return DatePartitionedTable(
            table_id=self.options.out_segmented_messages_table,
            schema=message_schema.message_output_schema,
            description=f"Created by pipe-segment:{__version__}.\n"
                        "Daily satellite messages segmented processed in segment step.",
            partitioning_field="timestamp",
        )

    @property
    def segments_output_table(self) -> DatePartitionedTable:
        return DatePartitionedTable(
            table_id=self.options.out_segments_table,
            schema=segment_schema.segment_schema,
            description=f"Created by pipe-segment:{__version__}.\n"
                        "Daily segments processed in segment step.",
            partitioning_field="timestamp",
        )

    @property
    def fragments_output_table(self) -> DatePartitionedTable:
        return DatePartitionedTable(
            table_id=self.options.out_fragments_table or self.options.fragments_table,
            schema=Fragment.schema,
            description=f"Created by pipe-segment:{__version__}.\n"
                        "Daily fragments processed in segment step.",
            partitioning_field="timestamp",
        )

    @property
    def prior_segments_table(self) -> str:
        """Table to read the segments of the day before the date range from."""
        return self.options.in_segments_table or self.options.out_segments_table

    @property
    def prior_fragments_table(self) -> str:
        """Table to read the fragments of the day before the date range from.

        It follows the same rule as :attr:`prior_segments_table`, an output table by default,
        so the segments are joined with the fragments of the same run.
        """
        return self.options.in_fragments_table or self.fragments_output_table.table_id

    @property
    def date_partitioned_output_tables(self) -> list:
        return [
            self.messages_output_table,
            self.segments_output_table,
            self.fragments_output_table,
        ]

    def segmenter(self, start_date, end_date):
        """Returns the transform that builds the segment map and the segments."""
        if self.options.incremental:
            return IncrementalSegments(
                prior_segments=ReadPriorSegments(
                    self.prior_segments_table,
                    self.prior_fragments_table,
                    start_date - dt.timedelta(days=1),
                    ssvid_filter_query=self.options.ssvid_filter_query,
                    read_from_bigquery_kwargs={"bigquery_job_labels": self.bq_helper.labels},
                ),
                merge_params=self.merge_params,
            )

        return FullHistorySegments(
            self.bq_helper,
            self.options.fragments_table,
            start_date,
            end_date,
            self.merge_params,
        )

    def write_to_date_partitioned_table(self, table: DatePartitionedTable):
        return beam.io.WriteToBigQuery(
            table=table.table_id,
            schema={"fields": table.schema},
            create_disposition=beam.io.BigQueryDisposition.CREATE_NEVER,
        )

    def ensure_output_tables(self):
        """Creates the output tables that do not exist yet. It does not delete any data."""
        for table in self.date_partitioned_output_tables:
            self.bq_helper.ensure_table_exists(table)

        if self.options.out_sat_offsets_table:
            SatelliteOffsetsWrite.ensure_output_table(self.options, self.cloud_options)

    def clear_output_tables(self, start_date, end_date):
        """Deletes the rows of the date range from the output tables.

        Args:
            start_date:
                First day to delete, included.

            end_date:
                Last day to delete, included.
        """
        for table in self.date_partitioned_output_tables:
            self.bq_helper.run_query(
                query=table.clear_query(start_date, end_date),
            )

        if self.options.out_sat_offsets_table:
            SatelliteOffsetsWrite.clear_output_table(self.options, self.cloud_options)

    def prepare_output_tables(self, start_date, end_date):
        """Gets the output tables ready for the date range, and runs the checks.

        The tables must exist for the checks to read them (on the first run they do not),
        and the date range is cleared last, so a failed check does not delete anything.

        Args:
            start_date:
                First day to process, included.

            end_date:
                Last day to process, included.
        """
        logger.info("Ensuring the output tables exist")
        self.ensure_output_tables()

        logger.info("Running the checks")
        for hook in self.precondition_hooks(start_date, end_date):
            hook(self)

        logger.info("Clearing the date range of the output tables")
        self.clear_output_tables(start_date, end_date)

    def precondition_hooks(self, start_date, end_date) -> list:
        """Returns the checks to run before the output tables are cleared.

        Only an incremental run depends on the data written by previous runs.

        Args:
            start_date:
                First day to process, included.

            end_date:
                Last day to process, included.

        Returns:
            The hooks, in the order to run them. There are none if the run is not incremental.
        """
        if not self.options.incremental:
            return []

        labels = self.bq_helper.labels
        return [
            verify_messages_exist_hook(
                self.source_tables, start_date, end_date, labels=labels
            ),
            verify_prior_segments_hook(
                self.prior_segments_table,
                self.prior_fragments_table,
                start_date - dt.timedelta(days=1),
                labels=labels,
            ),
        ]

    # TODO: consider breaking up
    def pipeline(self):
        start_date = safe_date(self.date_range[0])
        end_date = safe_date(self.date_range[1])

        read_messages = ReadMessages(
            bq_helper=self.bq_helper,
            sources=self.source_tables,
            start_date=start_date,
            end_date=end_date,
            ssvid_filter_query=self.options.ssvid_filter_query,
        )
        segmenter = self.segmenter(start_date, end_date)

        logger.info("Preparing output tables")
        self.prepare_output_tables(start_date, end_date)

        logger.info("Creating pipeline object...")
        pipeline = beam.Pipeline(options=self.beam_options)

        logger.info("Adding ReadMessages transform...")
        messages = (
            pipeline
            | read_messages
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
                logger.info("Adding SatelliteOffsetsWrite transform...")
                _ = (
                    satellite_offsets
                    | SatelliteOffsetsWrite(self.options)
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
        _ = new_fragments | "WriteFragments" >> self.write_to_date_partitioned_table(
            self.fragments_output_table)

        logger.info("Adding Segmenter transform...")
        segmented = new_fragments | "Segmenter" >> segmenter
        segmap_src = segmented[SEGMAP]

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

        _ = (
            {"segmap": msg_segmap, "target": tagged_messages}
            | "GroupMsgsWithMap" >> beam.CoGroupByKey()
            | "TagMsgsWithSegId" >> TagWithSegId()
            | "WhitelistFields" >> WhitelistFields()
            | "WriteMessages" >> self.write_to_date_partitioned_table(self.messages_output_table)
        )

        logger.info("Adding WriteSegments transform...")
        _ = segmented[SEGMENTS] | "WriteSegments" >> self.write_to_date_partitioned_table(
            self.segments_output_table
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
        else:
            success_states.add(PipelineState.RUNNING)
            success_states.add(PipelineState.UNKNOWN)
            success_states.add(PipelineState.PENDING)

        logger.info("returning with result.state=%s" % result.state)

        return 0 if result.state in success_states else 1


def run(*args, **kwargs):
    return SegmentPipeline.build(*args, **kwargs).run()
