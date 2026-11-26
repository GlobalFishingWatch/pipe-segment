"""Configuration of the segment info table."""

from dataclasses import dataclass

from gfw.common.bigquery.table_config import TableConfig
from gfw.common.bigquery.table_description import TableDescription


SUMMARY = """\
Identity of each segment summarized over its whole history, with one row per seg_id.
The identities of its days are summed and reduced to the most common value of each field, and
the vessel_id is the latest one that was assigned to the segment.
"""

CAVEATS = """\
⬖ vessel_id is the latest one assigned to the segment (the one of its most recent day), while
the identity fields summarize its whole history, so they can differ from the identity that the
vessel_id was derived from.
⬖ Segments without rows in segment_vessel_daily are left out.
⬖ Noisy segments are included: filter by pos_count to leave out the ones with few positions.
⬖ msg_count adds the identity messages to the positional ones, but a message can have both, so
it can be counted twice.
⬖ Each identity field is its most common value, or NULL when that value accounts for at most
most_common_min_freq of the identity messages. Messages without a valid value for the field
count in that total.
"""


@dataclass
class SegmentInfoTableDescription(TableDescription):
    """Description of the segment info table."""

    repo_name: str = "pipe-segment"
    title: str = "SEGMENT INFO"
    subtitle: str = "Identity of each segment summarized over its whole history"
    summary: str = SUMMARY
    caveats: str = CAVEATS


@dataclass
class SegmentInfoTableConfig(TableConfig):
    """Configuration of the segment info table."""

    schema_file: str = "segment_info.schema.json"
    clustering_fields: tuple[str, ...] = ("seg_id",)
