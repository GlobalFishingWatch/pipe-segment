"""Configuration of the segment vessel table."""

from dataclasses import dataclass

from gfw.common.bigquery.table_config import TableConfig
from gfw.common.bigquery.table_description import TableDescription


SUMMARY = """\
Mapping of the segments to the vessel_ids they have had, summarized over their whole history.
It is built from the daily vessel_ids of the segments.
"""

CAVEATS = """\
⬖ The table is rebuilt from scratch on every run.
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
