"""Entry point of the segment-info pipeline."""

from typing import Any

from pipe_segment.pipelines.segment_info.config import SegmentInfoConfig
from pipe_segment.segment_info.segment_info import SegmentInfoPipeline


def run(config: SegmentInfoConfig, **kwargs: Any) -> None:
    """Runs the segment-info pipeline.

    Args:
        config:
            The resolved configuration.

        **kwargs:
            Extra arguments of the CLI, which this pipeline does not use.
    """
    SegmentInfoPipeline(config).run()
