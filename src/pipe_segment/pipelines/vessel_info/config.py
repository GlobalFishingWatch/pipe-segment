"""Configuration of the vessel-info pipeline."""

from __future__ import annotations

from dataclasses import dataclass

from gfw.common.config import PipelineConfig, PipelineConfigError


ERROR_MOST_COMMON_MIN_FREQ = "most_common_min_freq must be between 0 and 1, but it is {}."


@dataclass(frozen=True, kw_only=True)
class VesselInfoConfig(PipelineConfig):
    """Configuration of the vessel-info pipeline."""

    project: str
    """GCP project that runs the queries and is billed for them."""

    bq_in_segment_identity_daily: str
    """Table to read the daily identities of the segments from."""

    bq_in_segment_vessel_daily: str
    """Table to read the daily vessel_ids of the segments from."""

    bq_out_vessel_info: str
    """Table in which to store the vessel info."""

    most_common_min_freq: float = 0.05
    """Minimal frequency that defines a ssvid that has a single dominant identity."""

    def __post_init__(self) -> None:
        """Validates the configuration.

        Raises:
            :class:`PipelineConfigError`:
                If :attr:`most_common_min_freq` is not between 0 and 1.
        """
        super().__post_init__()
        if not 0 <= self.most_common_min_freq <= 1:
            raise PipelineConfigError(ERROR_MOST_COMMON_MIN_FREQ.format(self.most_common_min_freq))
