"""Queries of the segment-vessel pipeline."""

import dataclasses

from dataclasses import dataclass

from gfw.common.query import Query


@dataclass
class SegmentVesselQuery(Query):
    """Segment vessel: one row per seg_id and vessel_id the segment has had.

    Its fields are the template's variables (see template_vars).
    """

    segment_vessel_daily: str

    @property
    def template_filename(self) -> str:
        """The template's file, in assets/queries."""
        return "segment_vessel.sql.j2"

    @property
    def template_vars(self) -> dict:
        """The template's variables: the query's fields."""
        return dataclasses.asdict(self)
