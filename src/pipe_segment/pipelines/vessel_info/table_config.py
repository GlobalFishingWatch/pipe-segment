"""Configuration of the vessel info table."""

from dataclasses import dataclass

from gfw.common.bigquery.table_config import TableConfig
from gfw.common.bigquery.table_description import TableDescription


SUMMARY = """\
Identity of each vessel_id summarized over its whole history, with one row per vessel_id.
It is built from the daily identities of the segments and the segments that each vessel_id
has had.
"""

CAVEATS = """\
⬖ The table is rebuilt from scratch on every run.
"""


@dataclass
class VesselInfoTableDescription(TableDescription):
    """Description of the vessel info table."""

    repo_name: str = "pipe-segment"
    title: str = "VESSEL INFO"
    subtitle: str = "Identity of each vessel_id summarized over its whole history"
    summary: str = SUMMARY
    caveats: str = CAVEATS


@dataclass
class VesselInfoTableConfig(TableConfig):
    """Configuration of the vessel info table."""

    schema_file: str = "vessel_info.schema.json"
