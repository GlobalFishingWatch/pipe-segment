"""Entry point of the vessel-info pipeline."""

from typing import Any

from pipe_segment.pipelines.vessel_info.config import VesselInfoConfig
from pipe_segment.vessel_info.vessel_info import VesselInfoPipeline


def run(config: VesselInfoConfig, **kwargs: Any) -> None:
    """Runs the vessel-info pipeline.

    Args:
        config:
            The resolved configuration.

        **kwargs:
            Extra arguments of the CLI, which this pipeline does not use.
    """
    VesselInfoPipeline(config).run()
