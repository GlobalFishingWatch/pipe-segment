import sys

from apache_beam.options.pipeline_options import TypeOptions
# Suppress a spurious warning that happens when you import apache_beam
from pipe_tools.beam import logging_monkeypatch
from pipe_tools.options import validate_options
from pipe_tools.options import LoggingOptions

from pipe_segment.options.stitcher import StitcherOptions
from pipe_segment import stitcher_pipeline

def run(args):
    args = args or []
    args.append('--no_pipeline_type_check')

    options = validate_options(args=args, option_classes=[LoggingOptions, StitcherOptions])

    options.view_as(LoggingOptions).configure_logging()

    return stitcher_pipeline.run(options)


if __name__ == '__main__':
    sys.exit(run(args=sys.argv))
