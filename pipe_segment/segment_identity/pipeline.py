################################################################################
# This file contains the segment identity pipeline. This is a hotfix for
# pipeline 2.5, and we won't be porting this fix to pipeline 3.0 an onwards, so
# I tried to make it self contained so that we don't have any merge conflicts
# or problems deleting this in the future.
#
# The purpose of this pipeline is to take the data that comes out of the new
# segmenter, which contains data per segment, and generate the
# `segment_identity_daily` table, which summarizes identity information per
# segment.
################################################################################
import logging

import apache_beam as beam
from apache_beam.runners import PipelineState
from apache_beam.options.pipeline_options import (
    GoogleCloudOptions, StandardOptions, PipelineOptions)

from pipe_segment.segment_identity.read_source import ReadSource
from pipe_segment.segment_identity.transforms import (rename_timestamp,
                                                      summarize_identifiers,
                                                      write_sink)
from pipe_segment.utils.bq_tools import BigQueryTools
from pipe_segment.version import __version__

from ..tools import timestamp_from_string, datetime_from_timestamp

logger = logging.getLogger(__name__)


DESCRIPTION_COUNT = "Number of times the unique field value occured for this segment for this day"
DESCRIPTION_SHIPNAME = "Array of unique shipnames (unnormalized) for this segment for this day."
DESCRIPTION_CALLSIGN = "Array of unique callsigns (unnormalized) for this segment for this day."
DESCRIPTION_IMO = "Array of unique imo numbers (unvalidated) for this segment for this day."
DESCRIPTION_SHIPNAME_NORM = "Array of unique normalized shipnames for this segment for this day."
DESCRIPTION_CALLSIGN_NORM = "Array of unique normalized callsigns for this segment for this day."
DESCRIPTION_IMO_VALID = "Array of all unique valid imo numbers for this segment for this day."

DEST_SEGMENT_IDENTITY_SCHEMA = {
    "fields": [
        {
            "mode": "NULLABLE",
            "name": "seg_id",
            "type": "STRING",
            "description": "unique segment id.  This table has one row per segment id per day",
        },
        {
            "mode": "NULLABLE",
            "name": "ssvid",
            "type": "STRING",
            "description": (
                "source specific vessel id. "
                "This is the transponder id, and for AIS this is the MMSI"
            ),
        },
        {
            "mode": "NULLABLE",
            "name": "summary_timestamp",
            "type": "TIMESTAMP",
            "description": "Timestamp this summary was created",
        },
        {
            "mode": "NULLABLE",
            "name": "first_timestamp",
            "type": "TIMESTAMP",
            "description": "Timestamp of the first message in the segment for this day",
        },
        {
            "mode": "NULLABLE",
            "name": "last_timestamp",
            "type": "TIMESTAMP",
            "description": "Timestamp of the last message in the segment for this day",
        },
        {
            "mode": "NULLABLE",
            "name": "first_pos_timestamp",
            "type": "TIMESTAMP",
            "description": "Timestamp of the first position message in the segment for this day",
        },
        {
            "mode": "NULLABLE",
            "name": "last_pos_timestamp",
            "type": "TIMESTAMP",
            "description": "Timestamp of the last position message in the segment for this day",
        },
        {
            "mode": "NULLABLE",
            "name": "msg_count",
            "type": "INTEGER",
            "description": (
                "Total number of messages (positional and identity messages) "
                "in the segment for this day"
            ),
        },
        {
            "mode": "NULLABLE",
            "name": "pos_count",
            "type": "INTEGER",
            "description": "Number of positional messages in the segment for this day",
        },
        {
            "mode": "NULLABLE",
            "name": "ident_count",
            "type": "INTEGER",
            "description": (
                "Number of identity messages in the segment for this day. "
                "Note that some messages can contain both position and identity"
            ),
        },
        {
            "fields": [
                {
                    "mode": "NULLABLE",
                    "name": "value",
                    "type": "STRING",
                    "description": "Unique field value",
                },
                {
                    "mode": "NULLABLE",
                    "name": "count",
                    "type": "INTEGER",
                    "description": DESCRIPTION_COUNT,
                }
            ],
            "mode": "REPEATED",
            "name": "shipname",
            "type": "RECORD",
            "description": DESCRIPTION_SHIPNAME,
        },
        {
            "fields": [
                {
                    "mode": "NULLABLE",
                    "name": "value",
                    "type": "STRING",
                    "description": "Unique field value",
                },
                {
                    "mode": "NULLABLE",
                    "name": "count",
                    "type": "INTEGER",
                    "description": DESCRIPTION_COUNT,
                },
            ],
            "mode": "REPEATED",
            "name": "callsign",
            "type": "RECORD",
            "description": DESCRIPTION_CALLSIGN,
        },
        {
            "fields": [
                {
                    "mode": "NULLABLE",
                    "name": "value",
                    "type": "STRING",
                    "description": "Unique field value",
                },
                {
                    "mode": "NULLABLE",
                    "name": "count",
                    "type": "INTEGER",
                    "description": DESCRIPTION_COUNT,
                },
            ],
            "mode": "REPEATED",
            "name": "imo",
            "type": "RECORD",
            "description": DESCRIPTION_IMO,
        },
        {
            "fields": [
                {
                    "mode": "NULLABLE",
                    "name": "value",
                    "type": "STRING",
                    "description": "Unique field value",
                },
                {
                    "mode": "NULLABLE",
                    "name": "count",
                    "type": "INTEGER",
                    "description": DESCRIPTION_COUNT,
                },
            ],
            "mode": "REPEATED",
            "name": "n_shipname",
            "type": "RECORD",
            "description": DESCRIPTION_SHIPNAME_NORM,
        },
        {
            "fields": [
                {
                    "mode": "NULLABLE",
                    "name": "value",
                    "type": "STRING",
                    "description": "Unique field value",
                },
                {
                    "mode": "NULLABLE",
                    "name": "count",
                    "type": "INTEGER",
                    "description": DESCRIPTION_COUNT,
                },
            ],
            "mode": "REPEATED",
            "name": "n_callsign",
            "type": "RECORD",
            "description": DESCRIPTION_CALLSIGN_NORM,
        },
        {
            "fields": [
                {
                    "mode": "NULLABLE",
                    "name": "value",
                    "type": "STRING",
                    "description": "Unique field value",
                },
                {
                    "mode": "NULLABLE",
                    "name": "count",
                    "type": "INTEGER",
                    "description": DESCRIPTION_COUNT,
                },
            ],
            "mode": "REPEATED",
            "name": "n_imo",
            "type": "RECORD",
            "description": DESCRIPTION_IMO_VALID,
        },
        {
            "fields": [
                {
                    "mode": "NULLABLE",
                    "name": "value",
                    "type": "STRING",
                    "description": "Unique field value",
                },
                {
                    "mode": "NULLABLE",
                    "name": "count",
                    "type": "INTEGER",
                    "description": DESCRIPTION_COUNT,
                },
            ],
            "mode": "REPEATED",
            "name": "length",
            "type": "RECORD",
            "description": "Array of all unique length for this segment for this day.",
        },
        {
            "fields": [
                {
                    "mode": "NULLABLE",
                    "name": "value",
                    "type": "STRING",
                    "description": "Unique field value",
                },
                {
                    "mode": "NULLABLE",
                    "name": "count",
                    "type": "INTEGER",
                    "description": DESCRIPTION_COUNT,
                },
            ],
            "mode": "REPEATED",
            "name": "width",
            "type": "RECORD",
            "description": "Array of all unique width for this segment for this day.",
        },
    ]
}


def parse_date_range(s):
    # parse a string YYYY-MM-DD,YYYY-MM-DD into 2 timestamps
    return list(map(timestamp_from_string, s.split(",")) if s is not None else (None, None))


class SegmentIdentityPipeline:
    def __init__(self, options, beam_options):
        self.options = options
        self.beam_options = beam_options
        self.cloud_options = beam_options.view_as(GoogleCloudOptions)
        self.date_range = parse_date_range(self.options.date_range)
        self.bqtools = BigQueryTools.build(project=self.cloud_options.project)

    @classmethod
    def build(cls, options, beam_args):
        beam_options = PipelineOptions(beam_args)

        return cls(options, beam_options)

    @property
    def temp_gcs_location(self):
        return self.cloud_options.temp_location

    @property
    def dest_segment_identity_schema(self):
        return DEST_SEGMENT_IDENTITY_SCHEMA

    def source_segments(self):
        from_ts, to_ts = self.date_range
        return ReadSource(
            self.options.source_segments,
            self.options.source_fragments,
            from_ts,
            to_ts)

    @property
    def summarize_identifiers(self):
        return beam.Map(summarize_identifiers)

    @property
    def rename_timestamp(self):
        return beam.Map(rename_timestamp)

    @property
    def destination_tables(self):
        return dict(
            segment_identity={
                "table": self.options.dest_segment_identity,
                "schema": self.dest_segment_identity_schema,
                "description": f"""Created by the pipe-segment: {__version__}.
                Daily segments identity processed in segment step.""",
            })

    @property
    def dest_segment_identity(self):
        return write_sink(
            self.destination_tables["segment_identity"]["table"],
            self.destination_tables["segment_identity"]["schema"],
            self.destination_tables["segment_identity"]["description"]
        )

    def pipeline(self):
        pipeline = beam.Pipeline(options=self.beam_options)

        start_dt, end_dt = [datetime_from_timestamp(ts) for ts in self.date_range]
        self.bqtools.create_or_clear_tables(
            self.destination_tables, start_dt.date(), end_dt.date(),
            date_field="summary_timestamp"
        )

        (
            pipeline
            | "ReadDailySegments" >> self.source_segments()
            | "SummarizeIdentifiers" >> self.summarize_identifiers
            | "RenameTimestamp" >> self.rename_timestamp
            | "WriteSegmentIdentity" >> self.dest_segment_identity
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
    return SegmentIdentityPipeline.build(*args, **kwargs).run()
