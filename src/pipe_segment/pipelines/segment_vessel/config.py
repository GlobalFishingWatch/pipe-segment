"""Configuration of the segment-vessel pipeline."""

from __future__ import annotations

from dataclasses import dataclass

from gfw.common.config import PipelineConfig


@dataclass(frozen=True, kw_only=True)
class SegmentVesselConfig(PipelineConfig):
    """Configuration of the segment-vessel pipeline."""

    project: str
    """GCP project that runs the queries and is billed for them."""

    bq_in_segment_vessel_daily: str
    """Table to read the daily vessel_ids of the segments from."""

    bq_out_segment_vessel: str
    """Table in which to store the segment-vessel mapping."""
