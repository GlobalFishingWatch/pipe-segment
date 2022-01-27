import sys

from pipe_tools.options import validate_options
from pipe_tools.options import LoggingOptions

from pipe_segment.options.frag2seg import Frag2SegOptions
from pipe_segment import frag2seg_pipeline


def run(args):
    args = args or []
    args.append("--no_pipeline_type_check")

    options = validate_options(
        args=args, option_classes=[LoggingOptions, Frag2SegOptions]
    )

    options.view_as(LoggingOptions).configure_logging()

    return frag2seg_pipeline.run(options)


if __name__ == "__main__":
    sys.exit(run(args=sys.argv))
