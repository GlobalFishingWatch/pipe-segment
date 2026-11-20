"""Entry point of the segment-vessel pipeline."""

from typing import Any

from pipe_segment.pipelines.segment_vessel.config import SegmentVesselConfig
from pipe_segment.segment_vessel.segment_vessel import SegmentVesselPipeline


def run(config: SegmentVesselConfig, **kwargs: Any) -> None:
    """Runs the segment-vessel pipeline.

    Args:
        config:
            The resolved configuration.

        **kwargs:
            Extra arguments of the CLI, which this pipeline does not use.
    """
    SegmentVesselPipeline(config).run()
