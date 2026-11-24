"""Queries of the segment-identity-daily pipeline."""

import dataclasses
import datetime

from dataclasses import dataclass

from gfw.common.query import Query


@dataclass
class SegmentsQuery(Query):
    """Segments of each day in [start_date, end_date), with the times of their fragments.

    Its fields are the template's variables (see template_vars).
    """

    source_segments: str
    source_fragments: str
    start_date: datetime.date
    end_date: datetime.date

    @property
    def template_filename(self) -> str:
        """The template's file, in assets/queries."""
        return "segment_identity_daily.sql.j2"

    @property
    def template_vars(self) -> dict:
        """The template's variables: the query's fields."""
        return dataclasses.asdict(self)
