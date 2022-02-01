import sys

from pipe_tools.options import validate_options
from pipe_tools.options import LoggingOptions

from pipe_segment.options.segment import SegmentOptions
from pipe_segment import segment_pipeline


def run(args):
    args = args or []
    args.append("--no_pipeline_type_check")

    options = validate_options(
        args=args, option_classes=[LoggingOptions, SegmentOptions]
    )

    options.view_as(LoggingOptions).configure_logging()

    return segment_pipeline.run(options)


if __name__ == "__main__":
    sys.exit(run(args=sys.argv))
