import logging
import apache_beam as beam
from apache_beam.runners import PipelineState
from apache_beam.options.pipeline_options import StandardOptions
from apache_beam.options.pipeline_options import GoogleCloudOptions

from .timestamp import as_timestamp
from .timestamp import datetimeFromTimestamp

from pipe_segment.options.frag2seg import Frag2SegOptions
from pipe_segment.transform.create_segments import CreateSegments
from pipe_segment.transform.read_fragments import ReadFragments
from pipe_segment.transform.write_segment_map import WriteSegmentMap


def parse_date_range(s):
    # parse a string YYYY-MM-DD,YYYY-MM-DD into 2 timestamps
    return list(map(as_timestamp, s.split(",")) if s is not None else (None, None))


def safe_dateFromTimestamp(ts):
    if ts is None:
        return None
    return datetimeFromTimestamp(ts).date()


def check_format(msg):
    assert set(msg.keys()) == {"seg_id", "frag_id"}, set(msg.keys())
    for v in msg.values():
        assert isinstance(v, str)
    return msg


class Frag2SegPipeline:
    def __init__(self, options):
        self.options = options.view_as(Frag2SegOptions)
        self.date_range = parse_date_range(self.options.date_range)

    @staticmethod
    def add_ssvid_key(msg):
        return (msg["ssvid"], msg)

    @property
    def project(self):
        return self.options.view_as(GoogleCloudOptions).project

    @property
    def temp_gcs_location(self):
        return self.options.view_as(GoogleCloudOptions).temp_location

    def pipeline(self):
        pipeline = beam.Pipeline(options=self.options)

        start_date = safe_dateFromTimestamp(self.date_range[0])
        end_date = safe_dateFromTimestamp(self.date_range[1])
        fragment_table = self.options.frag_source
        seg_map_table = self.options.seg_map_dest

        (
            pipeline
            | ReadFragments(fragment_table, start_date, end_date)
            | "AddSsvidKey" >> beam.Map(self.add_ssvid_key)
            | "GroupBySsvid" >> beam.GroupByKey()
            | CreateSegments()  # TODO: pass in args like segments?
            | beam.Map(check_format)
            | WriteSegmentMap(
                seg_map_table,
                self.project,
                start_date,
                end_date,
                self.temp_gcs_location,
            )
        )

        return pipeline

    def run(self):
        return self.pipeline().run()


def run(options):

    pipeline = Frag2SegPipeline(options)
    result = pipeline.run()

    success_states = set([PipelineState.DONE])

    if (
        pipeline.options.wait_for_job
        or options.view_as(StandardOptions).runner == "DirectRunner"
    ):
        result.wait_until_finish()
    else:
        success_states.add(PipelineState.RUNNING)
        success_states.add(PipelineState.UNKNOWN)
        success_states.add(PipelineState.PENDING)

    logging.info("returning with result.state=%s" % result.state)
    return 0 if result.state in success_states else 1
