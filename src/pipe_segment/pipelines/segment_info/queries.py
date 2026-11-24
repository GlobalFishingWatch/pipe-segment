"""Queries of the segment-info pipeline."""

import dataclasses

from dataclasses import dataclass

from gfw.common.query import Query


@dataclass
class SegmentInfoQuery(Query):
    """Segment info: one row per seg_id, summarizing its identity over the whole history.

    Its fields are the template's variables (see template_vars).
    """

    segment_identity_daily: str
    segment_vessel_daily: str
    most_common_min_freq: float

    @property
    def template_filename(self) -> str:
        """The template's file, in assets/queries."""
        return "segment_info.sql.j2"

    @property
    def template_vars(self) -> dict:
        """The template's variables: the query's fields."""
        return dataclasses.asdict(self)
