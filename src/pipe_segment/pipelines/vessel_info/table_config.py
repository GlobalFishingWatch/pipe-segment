"""Configuration of the vessel info table."""

from dataclasses import dataclass

from gfw.common.bigquery.table_config import TableConfig
from gfw.common.bigquery.table_description import TableDescription


SUMMARY = """\
Identity of each vessel_id summarized over its whole history, with one row per vessel_id.
Each segment is attributed to its latest vessel_id, and the identities of its days are summed
and reduced to the most common value of each field.
"""

CAVEATS = """\
⬖ A segment is counted entirely under its latest vessel_id (the one of its most recent day), not
day by day. If a segment changed its vessel_id, its whole history goes to the latest one, and a
vessel_id that is not the latest of any segment is not in the table.
⬖ Segments without rows in segment_vessel_daily are left out.
⬖ Noisy segments are included: nothing is filtered by pos_count, so segments with few positions
add to the counts and the identities.
⬖ msg_count adds the identity messages to the positional ones, but a message can have both, so
it can be counted twice.
⬖ Each identity field is its most common value, or NULL when that value accounts for at most
most_common_min_freq of the identity messages. Messages without a valid value for the field
count in that total.
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
