"""Configuration of the tables of the messages-segmented pipeline."""

from dataclasses import dataclass
from datetime import date
from typing import Optional

from gfw.common.bigquery.table_config import TableConfig
from gfw.common.bigquery.table_description import TableDescription


MESSAGES_SUMMARY = """\
Messages of the vessels, one row per message, tagged with the segment and the fragment that
they belong to. They are the normalized messages, with invalid values set to NULL and without
the messages of satellites with a bad clock.
"""

MESSAGES_CAVEATS = """\
⬖ Invalid values are set to NULL, not dropped: out of range lon, lat, course, heading and speed
of AIS and VMS messages, positions at 0,0 (both set to NULL), invalid IMO numbers, and callsigns,
shipnames and destinations made only of "@". Message types without rules are not changed.
⬖ If the inputs of the satellite offsets are given, the messages of a receiver are dropped when
its clock is off by more than max_timing_offset_s in the UTC hour of the message, padded by
bad_hour_padding hours on both sides. They are in none of the tables of the segmentation.
⬖ seg_id and frag_id are NULL in the messages of noise fragments, and seg_id is also NULL in
the messages that no segment matches.
⬖ There is no unique key: the msgid is not always unique, and several sources are put together
without deduplicating them.
⬖ Only the fields of the schema are kept.
"""

FRAGMENTS_SUMMARY = """\
Pieces of the track of a vessel, with one row per ssvid, frag_id and UTC day. A fragment is
built from the messages of a single day. Its identities and destinations are counted over them.
"""

FRAGMENTS_CAVEATS = """\
⬖ timestamp is midnight UTC of the day, not the time of a message. first_msg_* and last_msg_* are
the first and the last messages of that day.
⬖ Noise fragments are not written: their messages are in the segmented messages table, with a
NULL frag_id.
⬖ Messages dropped or set to NULL upstream (see the segmented messages table) are not part of the
fragments.
⬖ Later runs read this table back to match the segments, so it is part of the state of the
segmentation.
"""

SEGMENTS_SUMMARY = """\
Segments of the track of a vessel, with one row per seg_id and UTC day in which it has a
fragment. A row has the last fragment of the day, with the identities and destinations of the
day, and the ones accumulated since the segment started.
"""

SEGMENTS_CAVEATS = """\
⬖ timestamp is midnight UTC of the day, not the time of a message, and frag_id is the fragment
of that day.
⬖ A fragment joins an open segment only if the gap between the last message of the segment and
its first message is more than 0 and less than 24 hours, the best match first. Otherwise it
starts a new segment, with seg_id equal to its frag_id. A segment is open only if it had a
fragment the day before.
⬖ daily_* fields count the day, and cumulative_* fields count since the first day of the segment.
⬖ The identity fields are STRING, including length and width, which are FLOAT in the fragments.
⬖ An incremental run continues from the segments and fragments of the day before the date range,
which must be in the tables. A full history run builds them again from all the fragments before
the range.
"""

SATELLITE_OFFSETS_SUMMARY = """\
Offset of the clock of each satellite, with one row per receiver and hour: how many seconds it
differs from the median of the clocks of all the satellites, from the hour on, for sixty minutes.
"""

SATELLITE_OFFSETS_CAVEATS = """\
⬖ Only the hours with an offset have a row. Offsets use the position messages of satellite
receivers with a speed between 5 and 10, one per vessel and minute, and a pair of receivers needs
at least 10 of them per day.
⬖ dt is a median of medians: first per pair of receivers and hour, then per receiver and hour.
⬖ avg_distance_from_sat_km and med_dist_from_sat_km are NULL when there is no position of the
satellite at the exact second of the message, and dt supersedes them.
⬖ The offsets are computed again in every run, from the messages of the date range. The table
only records what the filter of the bad times used.
"""


@dataclass
class SegmentedMessagesTableDescription(TableDescription):
    """Description of the segmented messages table."""

    repo_name: str = "pipe-segment"
    title: str = "SEGMENTED MESSAGES"
    subtitle: str = "Messages of each vessel, tagged with their segment and fragment"
    summary: str = MESSAGES_SUMMARY
    caveats: str = MESSAGES_CAVEATS


@dataclass
class FragmentsTableDescription(TableDescription):
    """Description of the fragments table."""

    repo_name: str = "pipe-segment"
    title: str = "FRAGMENTS"
    subtitle: str = "Pieces of the track of a vessel, per day"
    summary: str = FRAGMENTS_SUMMARY
    caveats: str = FRAGMENTS_CAVEATS


@dataclass
class SegmentsTableDescription(TableDescription):
    """Description of the segments table."""

    repo_name: str = "pipe-segment"
    title: str = "SEGMENTS"
    subtitle: str = "Segments of the track of a vessel, per day"
    summary: str = SEGMENTS_SUMMARY
    caveats: str = SEGMENTS_CAVEATS


@dataclass
class SatelliteOffsetsTableDescription(TableDescription):
    """Description of the satellite offsets table."""

    repo_name: str = "pipe-segment"
    title: str = "SATELLITE OFFSETS"
    subtitle: str = "Offset of the clock of each satellite, per hour"
    summary: str = SATELLITE_OFFSETS_SUMMARY
    caveats: str = SATELLITE_OFFSETS_CAVEATS


@dataclass
class DatePartitionedTableConfig(TableConfig):
    """Configuration of a table partitioned by month on a timestamp, clustered by it.

    Same layout as the legacy pipeline's output tables.
    """

    schema_file: str = ""  # Each table sets its own.
    partition_type: str = "MONTH"
    partition_field: str = "timestamp"

    def __post_init__(self) -> None:
        """Clusters the table by the field that partitions it."""
        self.clustering_fields = (self.partition_field,)

    def delete_query(self, start_date: date, end_date: Optional[date] = None) -> str:
        """Returns the query that deletes the rows of the days in [start_date, end_date).

        The pipeline appends, so the range is cleared first to make reprocessing it idempotent.
        Without end_date, it deletes everything from start_date on.
        """
        field = self.partition_field
        query = f"DELETE FROM `{self.table_id}` WHERE DATE({field}) >= '{start_date}'"
        if end_date is not None:
            query += f" AND DATE({field}) < '{end_date}'"

        return query


@dataclass
class SegmentedMessagesTableConfig(DatePartitionedTableConfig):
    """Configuration of the segmented messages table."""

    schema_file: str = "messages_segmented.schema.json"


@dataclass
class FragmentsTableConfig(DatePartitionedTableConfig):
    """Configuration of the fragments table."""

    schema_file: str = "fragments.schema.json"


@dataclass
class SegmentsTableConfig(DatePartitionedTableConfig):
    """Configuration of the segments table."""

    schema_file: str = "segments.schema.json"


@dataclass
class SatelliteOffsetsTableConfig(DatePartitionedTableConfig):
    """Configuration of the satellite offsets table."""

    schema_file: str = "satellite_offsets.schema.json"
    partition_field: str = "hour"
