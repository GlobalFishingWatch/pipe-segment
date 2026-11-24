"""Configuration of the segment identity daily table."""

from dataclasses import dataclass
from datetime import date
from typing import Optional

from gfw.common.bigquery.table_config import TableConfig
from gfw.common.bigquery.table_description import TableDescription


SUMMARY = """\
Identity of each segment, per day, with one row per seg_id and day.
For each identity field (shipname, callsign, imo, their normalized versions, length and width),
the different values that the messages of the segment broadcast that day, with the number of
times of each one.
"""

CAVEATS = """\
⬖ Only the days of segments that have a fragment on the same day are included: a segment row
without one is left out.
⬖ A field keeps only the valid values: shipnames and callsigns that normalize to nothing,
IMO numbers that are not valid, and missing lengths and widths are dropped. The n_ fields hold
the normalized values, so different values can add up in one. A field without valid values
is NULL.
⬖ ident_count is the sum of the counts of the identities of the segment, and msg_count adds it
to pos_count. A message that has both a position and an identity can be counted twice.
⬖ first_pos_timestamp and last_pos_timestamp are copies of first_timestamp and last_timestamp:
all of them are the first and last message times of the fragments.
"""


@dataclass
class SegmentIdentityDailyTableDescription(TableDescription):
    """Description of the segment identity daily table."""

    repo_name: str = "pipe-segment"
    title: str = "SEGMENT IDENTITY DAILY"
    subtitle: str = "Identity of each segment, per day"
    summary: str = SUMMARY
    caveats: str = CAVEATS


@dataclass
class SegmentIdentityDailyTableConfig(TableConfig):
    """Configuration of the segment identity daily table."""

    schema_file: str = "segment_identity_daily.schema.json"
    # Same layout as the legacy pipeline's output table.
    partition_type: str = "MONTH"
    partition_field: str = "summary_timestamp"
    clustering_fields: tuple[str, ...] = ("summary_timestamp",)

    def delete_query(self, start_date: date, end_date: Optional[date] = None) -> str:
        """Returns the query that deletes the rows of the days in [start_date, end_date).

        The pipeline appends, so the range is cleared first to make reprocessing it idempotent.
        Without end_date, it deletes everything from start_date on.
        """
        query = f"DELETE FROM `{self.table_id}` WHERE DATE(summary_timestamp) >= '{start_date}'"
        if end_date is not None:
            query += f" AND DATE(summary_timestamp) < '{end_date}'"

        return query
