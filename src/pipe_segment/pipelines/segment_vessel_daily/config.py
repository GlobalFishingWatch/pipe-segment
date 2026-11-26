"""Configuration of the segment-vessel-daily pipeline."""

from __future__ import annotations

from dataclasses import dataclass

from gfw.common.config import DatePipelineConfig, PipelineConfigError


ERROR_FREQUENCY = "{} must be between 0 and 1, but it is {}."
ERROR_NEGATIVE = "{} must not be negative, but it is {}."


@dataclass(frozen=True, kw_only=True)
class SegmentVesselDailyConfig(DatePipelineConfig):
    """Configuration of the segment-vessel-daily pipeline.

    Processes the days in [start_date, end_date).
    """

    project: str
    """GCP project that runs the queries and is billed for them."""

    bq_in_segment_identity_daily: str
    """Table to read the daily identities of the segments from."""

    bq_out_segment_vessel_daily: str
    """Table in which to store the daily vessel_ids of the segments."""

    window_days: int = 30
    """Days of daily segment identities that are windowed to identify a vessel_id."""

    single_ident_min_freq: float = 0.99
    """Minimum frequency of an identity value to be the single dominant one of a segment."""

    most_common_min_freq: float = 0.05
    """Minimal frequency that defines a ssvid that has a single dominant identity."""

    spoofing_threshold: int = 10
    """Amount of spoofing points from which a segment is considered noise."""

    dry_run: bool = False
    """If True, only executes the queries of the days, in dry run mode."""

    def __post_init__(self) -> None:
        """Validates the configuration.

        Raises:
            :class:`PipelineConfigError`:
                If a frequency is not between 0 and 1, or an amount of days or points is
                negative. It also raises the errors of :class:`DatePipelineConfig`.
        """
        super().__post_init__()
        for name in ("single_ident_min_freq", "most_common_min_freq"):
            value = getattr(self, name)
            if not 0 <= value <= 1:
                raise PipelineConfigError(ERROR_FREQUENCY.format(name, value))

        for name in ("window_days", "spoofing_threshold"):
            value = getattr(self, name)
            if value < 0:
                raise PipelineConfigError(ERROR_NEGATIVE.format(name, value))
