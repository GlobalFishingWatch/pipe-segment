"""Configuration of the segment-identity-daily pipeline."""

from __future__ import annotations

from dataclasses import dataclass

from gfw.common.config import DatePipelineConfig


@dataclass(frozen=True, kw_only=True)
class SegmentIdentityDailyConfig(DatePipelineConfig):
    """Configuration of the segment-identity-daily pipeline.

    Processes the days in [start_date, end_date).
    """

    project: str
    """GCP project that runs the pipeline and the queries, and is billed for them."""

    bq_in_segments: str
    """Table to read the segments from."""

    bq_in_fragments: str
    """Table to read the fragments from."""

    bq_out_segment_identity_daily: str
    """Table in which to store the daily identities of the segments."""

    wait_for_job: bool = False
    """Whether to wait until the job finishes before returning."""
