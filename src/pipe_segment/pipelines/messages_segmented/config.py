"""Configuration of the messages-segmented pipeline."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

from gfw.common.config import DatePipelineConfig, PipelineConfigError


ERROR_NEGATIVE = "{} must not be negative, but it is {}."
ERROR_BINS_PER_DAY = "bins_per_day must be at least 1, but it is {}."


@dataclass(frozen=True, kw_only=True)
class MessagesSegmentedConfig(DatePipelineConfig):
    """Configuration of the messages-segmented pipeline.

    Processes the days in [start_date, end_date).
    """

    project: str
    """GCP project that runs the pipeline and the queries, and is billed for them."""

    bq_in_normalized_messages: str
    """Table to read the normalized messages from."""

    bq_out_segmented_messages: str
    """Table in which to store the segmented messages."""

    bq_out_segments: str
    """Table in which to store the segments-days."""

    bq_out_fragments: str
    """Table in which to store the fragments."""

    bq_in_segments: Optional[str] = None
    """Table to read the segments of the day before the date range from, in incremental mode.

    Defaults to :attr:`bq_out_segments`.
    """

    bq_in_fragments: Optional[str] = None
    """Table to read the fragments from.

    A full-history run reads all the fragments before the date range, and an incremental run
    joins the segments of the day before the date range with the fragments of that day.
    Defaults to :attr:`bq_out_fragments`.
    """

    bq_in_sat_offset_messages: Optional[str] = None
    """Table to read the normalized messages that the satellite offsets are computed from."""

    bq_in_norad_to_receiver: Optional[str] = None
    """Table that links NORAD IDs and receivers."""

    bq_in_sat_positions: Optional[str] = None
    """Table with the distance to the satellite by receiver at 1s resolution."""

    bq_out_sat_offsets: Optional[str] = None
    """Table in which to store the satellite offsets."""

    bad_hour_padding: int = 1
    """Hours on either side of an hour with bad satellite timing to suppress."""

    max_timing_offset_s: int = 30
    """Maximum offset (in seconds) of a satellite clock before its messages are dropped."""

    bins_per_day: int = 4
    """Amount of containers per day to tag fragments and messages."""

    ssvid_filter_query: Optional[str] = None
    """Query that returns the ssvids to trim the messages down to."""

    incremental: bool = False
    """Whether to start from the segments of the day before the date range."""

    def __post_init__(self) -> None:
        """Validates the configuration.

        Raises:
            :class:`PipelineConfigError`:
                If :attr:`bad_hour_padding` or :attr:`max_timing_offset_s` is negative, or
                :attr:`bins_per_day` is less than 1. It also raises the errors of
                :class:`DatePipelineConfig`.
        """
        super().__post_init__()
        for name in ("bad_hour_padding", "max_timing_offset_s"):
            value = getattr(self, name)
            if value < 0:
                raise PipelineConfigError(ERROR_NEGATIVE.format(name, value))

        if self.bins_per_day < 1:
            raise PipelineConfigError(ERROR_BINS_PER_DAY.format(self.bins_per_day))

    @property
    def in_segments_table(self) -> str:
        """Table to read the segments of the day before the date range from."""
        return self.bq_in_segments or self.bq_out_segments

    @property
    def in_fragments_table(self) -> str:
        """Table to read the fragments from.

        It follows the same rule as :attr:`in_segments_table`, an output table by default,
        so the segments are joined with the fragments of the same run.
        """
        return self.bq_in_fragments or self.bq_out_fragments

    @property
    def sat_offsets_enabled(self) -> bool:
        """Whether the satellite timing correction runs: it needs all of its inputs."""
        return bool(
            self.bq_in_sat_offset_messages
            and self.bq_in_norad_to_receiver
            and self.bq_in_sat_positions
        )
