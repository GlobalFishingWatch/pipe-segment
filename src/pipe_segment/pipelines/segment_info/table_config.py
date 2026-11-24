"""Configuration of the segment info table."""

from dataclasses import dataclass

from gfw.common.bigquery.table_config import TableConfig
from gfw.common.bigquery.table_description import TableDescription


SUMMARY = """\
Identity of each segment summarized over its whole history, with one row per seg_id.
It is built from the daily identities of the segment and the vessel_ids that it has had.
"""

CAVEATS = """\
⬖ The table is rebuilt from scratch on every run.
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
