import sys

# Suppress a spurious warning that happens when you import apache_beam
from pipe_tools.beam import logging_monkeypatch
from pipe_tools.options import validate_options
from pipe_tools.options import LoggingOptions

from pipe_segment.options import SegmentOptions
from pipe_segment import pipeline

def run(args=None):
    options = validate_options(args=args, option_classes=[LoggingOptions,SegmentOptions])

    options.view_as(LoggingOptions).configure_logging()

    return pipeline.run(options)


if __name__ == '__main__':
    sys.exit(run(args=sys.argv))
