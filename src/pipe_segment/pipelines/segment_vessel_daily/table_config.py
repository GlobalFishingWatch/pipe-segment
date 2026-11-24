"""Configuration of the segment vessel daily table."""

from dataclasses import dataclass
from datetime import date
from typing import Optional

from gfw.common.bigquery.table_config import TableConfig
from gfw.common.bigquery.table_description import TableDescription


SUMMARY = """\
Vessel_id assigned to each segment, per day, with one row per seg_id and day.
The identities of the segments over a window of days are summarized to find the vessel that
each segment belongs to; the day is the last one of the window.
"""

CAVEATS = """\
⬖ A seg_id can have more than one vessel_id over time, but only one for any given day.
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
