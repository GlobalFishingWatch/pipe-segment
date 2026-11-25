"""Entry point of the messages-segmented pipeline."""

from typing import Any

from gfw.common.beam.pipeline import Pipeline

from pipe_segment.pipeline import SegmentPipeline
from pipe_segment.pipelines.messages_segmented.config import MessagesSegmentedConfig


def run(config: MessagesSegmentedConfig, **kwargs: Any) -> int:
    """Runs the messages-segmented pipeline.

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
    return SegmentPipeline(config, beam_pipeline.pipeline_options).run()
