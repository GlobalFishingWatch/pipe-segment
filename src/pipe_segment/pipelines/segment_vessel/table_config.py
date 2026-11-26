"""Configuration of the segment vessel table."""

from dataclasses import dataclass

from gfw.common.bigquery.table_config import TableConfig
from gfw.common.bigquery.table_description import TableDescription


SUMMARY = """\
Mapping of each segment to the vessel_ids it has had, with one row per seg_id and vessel_id.
It is built from segment_vessel_daily: for each pair, the number of days and the first and last
day in which the vessel_id was assigned to the segment.
"""

CAVEATS = """\
⬖ A segment can have several vessel_ids, as its identity changes over time, but only one per day.
⬖ vessel_id_rank 1 is the vessel_id with the latest last_date, not the one with the most days.
Ties go to the first vessel_id in alphabetical order.
⬖ days counts the days of segment_vessel_daily: days missing from that table are not counted.
"""


@dataclass
class SegmentVesselTableDescription(TableDescription):
    """Description of the segment vessel table."""

    repo_name: str = "pipe-segment"
    title: str = "SEGMENT VESSEL"
    subtitle: str = "Vessel_ids that each segment has had over its whole history"
    summary: str = SUMMARY
    caveats: str = CAVEATS


@dataclass
class SegmentVesselTableConfig(TableConfig):
    """Configuration of the segment vessel table."""

    schema_file: str = "segment_vessel.schema.json"
