import sys
from apache_beam.options.pipeline_options import GoogleCloudOptions
from pipe_segment.options.segment import SegmentOptions
from pipe_segment.options.validate_options import validate_options
from pipe_segment.options.logging_options import LoggingOptions
from pipe_segment.transform.satellite_offsets import remove_satellite_offsets_content
from pipe_segment import pipeline


def run(args):
    args = args or []

    options = validate_options(
        args=args, option_classes=[LoggingOptions, SegmentOptions]
    )

    options.view_as(LoggingOptions).configure_logging()

    seg_options = options.view_as(SegmentOptions)
    gcloud_options = options.view_as(GoogleCloudOptions)
    if seg_options.sat_offset_dest:
        remove_satellite_offsets_content(
            seg_options.sat_offset_dest,
            seg_options.date_range,
            gcloud_options.labels,
            gcloud_options.project)

    return pipeline.run(options)


if __name__ == "__main__":
    sys.exit(run(args=sys.argv[1:]))
