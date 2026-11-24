"""Queries of the messages-segmented pipeline."""

import dataclasses
import datetime

from dataclasses import dataclass
from typing import Optional

from gfw.common.query import Query


@dataclass
class PriorSegmentsQuery(Query):
    """State of the segments at the end of a day: one row per segment with a fragment on it.

    Its fields are the template's variables (see template_vars).
    """

    segments_table: str
    fragments_table: str
    day: datetime.date
    ssvid_filter_query: Optional[str] = None

    @property
    def template_filename(self) -> str:
        """The template's file, in assets/queries."""
        return "prior_segments.sql.j2"

    @property
    def template_vars(self) -> dict:
        """The template's variables: the query's fields."""
        return dataclasses.asdict(self)
