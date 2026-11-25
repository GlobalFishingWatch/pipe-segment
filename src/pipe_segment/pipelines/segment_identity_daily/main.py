"""Entry point of the segment-identity-daily pipeline."""

from typing import Any

from gfw.common.beam.pipeline import Pipeline

from pipe_segment.pipelines.segment_identity_daily.config import SegmentIdentityDailyConfig
from pipe_segment.segment_identity.pipeline import SegmentIdentityPipeline


def run(config: SegmentIdentityDailyConfig, **kwargs: Any) -> int:
    """Runs the segment-identity-daily pipeline.

    The Beam options come from the project and the labels of the config, the arguments that
    the CLI did not parse, and gfw-common's defaults.

    Args:
        config:
            The resolved configuration.

        **kwargs:
            Extra arguments of the CLI, which this pipeline does not use.

    Returns:
        0 if the job succeeded or is still running, 1 otherwise.
    """
    beam_pipeline = Pipeline(
        unparsed_args=config.unknown_unparsed_args,
        project=config.project,
        labels=config.labels,
        **config.unknown_parsed_args,
    )
    return SegmentIdentityPipeline(config, beam_pipeline.pipeline_options).run()
