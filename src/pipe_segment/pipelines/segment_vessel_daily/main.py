"""Entry point of the segment-vessel-daily pipeline."""

from typing import Any

from pipe_segment.pipelines.segment_vessel_daily.config import SegmentVesselDailyConfig
from pipe_segment.segment_vessel.segment_vessel_daily import SegmentVesselDailyPipeline


def run(config: SegmentVesselDailyConfig, **kwargs: Any) -> None:
    """Runs the segment-vessel-daily pipeline.

    Args:
        config:
            The resolved configuration.

        **kwargs:
            Extra arguments of the CLI, which this pipeline does not use.
    """
    SegmentVesselDailyPipeline(config).run()
