import sys

from pipe_tools.options import validate_options
from pipe_tools.options import LoggingOptions

from pipe_segment.options.segment import SegmentOptions
from pipe_segment import pipeline

def run(args):
    args = args or []

    options = validate_options(args=args, option_classes=[LoggingOptions,SegmentOptions])

    options.view_as(LoggingOptions).configure_logging()

    return pipeline.run(options)


if __name__ == '__main__':
    sys.exit(run(args=sys.argv))
