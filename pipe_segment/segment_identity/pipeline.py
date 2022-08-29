import datetime as dt

import apache_beam as beam
from apache_beam.options.pipeline_options import GoogleCloudOptions
from pipe_segment.segment_identity.options import SegmentIdentityOptions
from pipe_segment.segment_identity.read_source import ReadSource
from pipe_segment.segment_identity.transforms import (rename_timestamp,
                                                      summarize_identifiers,
                                                      write_sink)

from ..tools import as_timestamp


def parse_date_range(s):
    # parse a string YYYY-MM-DD,YYYY-MM-DD into 2 timestamps
    return list(map(as_timestamp, s.split(",")) if s is not None else (None, None))


timezoneToDatetime = lambda ts: dt.datetime.fromtimestamp(ts, tz=dt.timezone.utc)


class SegmentIdentityPipeline:
    def __init__(self, options):
        self.options = options.view_as(SegmentIdentityOptions)
        self.date_range = parse_date_range(self.options.date_range)

    @property
    def temp_gcs_location(self):
        return self.options.view_as(GoogleCloudOptions).temp_location

    @property
    def dest_segment_identity_schema(self):
        return {
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
                    "description": "source specific vessel id.  This is the transponder id, and for AIS this is the MMSI",
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
                    "description": "Total number of messages (positional and identity messages) in the segment for this day",
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
                    "description": "Number of identity messages in the segment for this day. Note that some messages can contain both position and identity",
                },
                {
                    "mode": "NULLABLE",
                    "name": "noise",
                    "type": "BOOLEAN",
                    "description": "If true, then this is a noise segment, usually because of an invalid lat or lon value.  Messages in these segments should be filtered out",
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
                            "description": "Number of times the unique field value occured for this segment for this day",
                        },
                    ],
                    "mode": "REPEATED",
                    "name": "shipname",
                    "type": "RECORD",
                    "description": "Array of all unique shipnames (unnormalized) for this segment for this day.",
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
                            "description": "Number of times the unique field value occured for this segment for this day",
                        },
                    ],
                    "mode": "REPEATED",
                    "name": "callsign",
                    "type": "RECORD",
                    "description": "Array of all unique callsigns (unnormalized) for this segment for this day.",
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
                            "description": "Number of times the unique field value occured for this segment for this day",
                        },
                    ],
                    "mode": "REPEATED",
                    "name": "imo",
                    "type": "RECORD",
                    "description": "Array of all unique imo numbers (unvalidated) for this segment for this day.",
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
                            "description": "Number of times the unique field value occured for this segment for this day",
                        },
                    ],
                    "mode": "REPEATED",
                    "name": "n_shipname",
                    "type": "RECORD",
                    "description": "Array of all unique normalized shipnames for this segment for this day.",
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
                            "description": "Number of times the unique field value occured for this segment for this day",
                        },
                    ],
                    "mode": "REPEATED",
                    "name": "n_callsign",
                    "type": "RECORD",
                    "description": "Array of all unique normalized callsigns for this segment for this day.",
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
                            "description": "Number of times the unique field value occured for this segment for this day",
                        },
                    ],
                    "mode": "REPEATED",
                    "name": "n_imo",
                    "type": "RECORD",
                    "description": "Array of all unique valid imo numbers for this segment for this day.",
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
                            "description": "Number of times the unique field value occured for this segment for this day",
                        },
                    ],
                    "mode": "REPEATED",
                    "name": "shiptype",
                    "type": "RECORD",
                    "description": "Array of all unique shiptypes for this segment for this day. Note that this field is already normalized in the messages",
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
                            "description": "Number of times the unique field value occured for this segment for this day",
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
                            "description": "Number of times the unique field value occured for this segment for this day",
                        },
                    ],
                    "mode": "REPEATED",
                    "name": "width",
                    "type": "RECORD",
                    "description": "Array of all unique width for this segment for this day.",
                },
            ]
        }

    def source_segments(self):
        from_ts, to_ts = self.date_range
        return ReadSource(self.options.source_segments, from_ts, to_ts)

    @property
    def summarize_identifiers(self):
        return beam.Map(summarize_identifiers)

    @property
    def rename_timestamp(self):
        return beam.Map(rename_timestamp)

    @property
    def dest_segment_identity(self):
        from_ts, _ = self.date_range
        return write_sink(
            self.options.dest_segment_identity,
            self.dest_segment_identity_schema,
            timezoneToDatetime(from_ts),
            "Daily segments identity processed in segment step.",
        )

    def pipeline(self):
        pipeline = beam.Pipeline(options=self.options)

        (
            pipeline
            | "ReadDailySegments" >> self.source_segments()
            | "SummarizeIdentifiers" >> self.summarize_identifiers
            | "RenameTimestamp" >> self.rename_timestamp
            | "WriteSegmentIdentity" >> self.dest_segment_identity
        )
        return pipeline

    def run(self):
        return self.pipeline().run()
