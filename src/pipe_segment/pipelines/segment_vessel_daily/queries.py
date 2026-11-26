"""Queries of the segment-vessel-daily pipeline."""

import dataclasses
import datetime

from dataclasses import dataclass

from gfw.common.query import Query


@dataclass
class SegmentVesselDailyQuery(Query):
    """Vessel_id of each segment for a day, from the identities in a window of days.

    Its fields are the template's variables (see template_vars).
    """

    date: datetime.date
    segment_identity: str
    window_days: int
    single_ident_min_freq: float
    most_common_min_freq: float
    spoofing_threshold: int

    @property
    def template_filename(self) -> str:
        """The template's file, in assets/queries."""
        return "segment_vessel_daily.sql.j2"

    @property
    def template_vars(self) -> dict:
        """The template's variables: the query's fields."""
        return dataclasses.asdict(self)
