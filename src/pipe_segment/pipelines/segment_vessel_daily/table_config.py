"""Configuration of the segment vessel daily table."""

from dataclasses import dataclass
from datetime import date
from typing import Optional

from gfw.common.bigquery.table_config import TableConfig
from gfw.common.bigquery.table_description import TableDescription


SUMMARY = """\
Vessel_id assigned to each segment, per day, with one row per seg_id and day.
A segment has a row for the days in which it has identity data. The identities of the window of
days that ends on the day are summarized to find the vessel that the segment belongs to.
"""

CAVEATS = """\
⬖ The counts and the timestamps (msg_count, pos_count, first_* and last_*) cover the window of
window_days days that ends on the day, not the day alone. Do not add them across days.
⬖ Each identity field is its most common value in the window, or NULL when that value accounts
for at most most_common_min_freq of the identity messages. Messages without a valid value for
the field count in that total.
⬖ vessel_id is a hash of the ssvid and the normalized imo, or of the ssvid, shipname and callsign
if there is no imo. A segment without identity gets the hash of its ssvid alone, the same as the
other segments of the ssvid without identity.
⬖ If the ssvid has a single dominant identity (above single_ident_min_freq) and its segments with
more than spoofing_threshold positions do not overlap in time, all its segments get the vessel_id
of the ssvid (single_ident_ssvid is true), instead of one derived from their own identity.
⬖ A day needs the previous window_days - 1 days of segment_identity_daily: with fewer, its window
is incomplete and it has less data.
"""


@dataclass
class SegmentVesselDailyTableDescription(TableDescription):
    """Description of the segment vessel daily table."""

    repo_name: str = "pipe-segment"
    title: str = "SEGMENT VESSEL DAILY"
    subtitle: str = "Vessel_id assigned to each segment, per day"
    summary: str = SUMMARY
    caveats: str = CAVEATS


@dataclass
class SegmentVesselDailyTableConfig(TableConfig):
    """Configuration of the segment vessel daily table."""

    schema_file: str = "segment_vessel_daily.schema.json"
    # Same layout as the legacy pipeline's output table.
    partition_type: str = "MONTH"
    partition_field: str = "day"
    clustering_fields: tuple[str, ...] = ("day",)

    def delete_query(self, start_date: date, end_date: Optional[date] = None) -> str:
        """Returns the query that deletes the rows of the days in [start_date, end_date).

        The pipeline appends, so the range is cleared first to make reprocessing it idempotent.
        Without end_date, it deletes everything from start_date on.
        """
        query = f"DELETE FROM `{self.table_id}` WHERE DATE(day) >= '{start_date}'"
        if end_date is not None:
            query += f" AND DATE(day) < '{end_date}'"

        return query
