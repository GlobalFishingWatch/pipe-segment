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


@dataclass
class MessagesQuery(Query):
    """Messages of a source table between two days, with the timestamp in seconds.

    The messages are filtered by the day of ``partition_field``, so the query only reads
    the partitions of the days. Its fields are the template's variables (see template_vars).
    """

    source: str
    start_date: datetime.date
    end_date: datetime.date
    ssvid_filter_query: Optional[str] = None
    partition_field: str = "timestamp"

    @property
    def template_filename(self) -> str:
        """The template's file, in assets/queries."""
        return "messages.sql.j2"

    @property
    def template_vars(self) -> dict:
        """The template's variables: the query's fields."""
        return dataclasses.asdict(self)


@dataclass
class MessageDaysQuery(Query):
    """Number of days with messages between two days, over all the source tables together.

    Its fields are the template's variables (see template_vars).
    """

    sources: list[str]
    start_date: datetime.date
    end_date: datetime.date
    partition_field: str = "timestamp"

    @property
    def template_filename(self) -> str:
        """The template's file, in assets/queries."""
        return "message_days.sql.j2"

    @property
    def template_vars(self) -> dict:
        """The template's variables: the query's fields."""
        return dataclasses.asdict(self)
