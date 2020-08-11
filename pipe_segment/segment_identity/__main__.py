################################################################################
# This file contains the segment identity pipeline. This is a hotfix for
# pipeline 2.5, and we won't be porting this fix to pipeline 3.0 an onwards, so
# I tried to make it self contained so that we don't have any merge conflicts
# or problems deleting this in the future.
#
# The purpose of this pipeline is to take the data that comes out of the new
# segmenter, which contains data per segment, and generate the
# `segment_identity_daily` table, which summarizes identity information per
# segment.
################################################################################
import logging
import sys

import apache_beam as beam
from apache_beam.runners import PipelineState
from apache_beam.options.pipeline_options import StandardOptions

from pipe_tools.options import LoggingOptions
from pipe_tools.options import validate_options

from pipe_segment.segment_identity.options import SegmentIdentityOptions
from pipe_segment.segment_identity.pipeline import SegmentIdentityPipeline

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)

    args = sys.argv or []
    args.append('--no_pipeline_type_check')

    options = validate_options(
        args=args, option_classes=[LoggingOptions, SegmentIdentityOptions])
    options.view_as(LoggingOptions).configure_logging()
    pipeline = SegmentIdentityPipeline(options)
    result = pipeline.run()

    success_states = set([PipelineState.DONE])

    if pipeline.options.wait_for_job or options.view_as(
            StandardOptions).runner == 'DirectRunner':
        logging.info("Waiting until job is done")
        result.wait_until_finish()
    else:
        success_states.add(PipelineState.RUNNING)
        success_states.add(PipelineState.UNKNOWN)
        success_states.add(PipelineState.PENDING)

    logging.info('returning with result.state=%s' % result.state)
    if result.state in success_states:
        exit(0)
    else:
        exit(1)

    exit(pipeline.run)

