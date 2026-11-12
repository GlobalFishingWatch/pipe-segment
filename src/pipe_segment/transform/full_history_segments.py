import datetime as dt

from typing import Dict

import apache_beam as beam

from pipe_segment.tools import datetime_from_timestamp
from pipe_segment.transform.create_segment_map import CreateSegmentMap
from pipe_segment.transform.create_segments import CreateSegments
from pipe_segment.transform.read_fragments import ReadFragments
from pipe_segment.transform.tag_with_seg_id import TagWithSegId
from pipe_segment.utils.bq_tools import BigQueryHelper


SEGMAP = "segmap"
SEGMENTS = "segments"


def timestamp_to_date(ts: float) -> dt.date:
    return datetime_from_timestamp(ts).date()


def strip_identity_and_destination(fragment):
    x = fragment.copy()
    x["identities"] = []
    x["destinations"] = []
    return x


class FullHistorySegments(beam.PTransform):
    """Builds the segment map and the segments from the whole history of fragments.

    It reads every fragment written before the date range, matches them all again and
    keeps only the segments-days of the date range.

    Args:
        bq_helper: helper to run queries and to label them.
        fragments_table: table to read the fragments written before the date range from.
        start_date: first day to write segments for.
        end_date: last day to write segments for.
        merge_params: parameters of the matcher.
    """

    def __init__(
        self,
        bq_helper: BigQueryHelper,
        fragments_table: str,
        start_date: dt.date,
        end_date: dt.date,
        merge_params: Dict,
    ):
        self.bq_helper = bq_helper
        self.fragments_table = fragments_table
        self.start_date = start_date
        self.end_date = end_date
        self.merge_params = merge_params

    def verify(self):
        """Nothing to check: it does not depend on the output of previous runs."""

    def in_date_range(self, segment):
        return self.start_date <= timestamp_to_date(segment["timestamp"]) <= self.end_date

    def expand(self, new_fragments):
        existing_fragments = new_fragments.pipeline | ReadFragments(
            self.bq_helper,
            self.fragments_table,
            # TODO should be able to use single lookback, but would have to
            # lookback at fragments not segments or something otherwise complicated
            start_date=None,  # start_date - timedelta(days=1),
            end_date=self.start_date - dt.timedelta(days=1),
        )

        all_fragments = (new_fragments, existing_fragments) | beam.Flatten()

        segmap = (
            all_fragments
            | "StripIdentity" >> beam.Map(strip_identity_and_destination)
            | "AddSsvidKey" >> beam.Map(lambda x: (x["ssvid"], x))
            | "GroupBySsvid" >> beam.GroupByKey()
            | CreateSegmentMap(self.merge_params)
        )

        frag_segmap = segmap | "AddFragidKey" >> beam.Map(lambda x: (x["frag_id"], x))
        tagged_fragments = all_fragments | "AddKeyToFragments" >> beam.Map(
            lambda x: (x["frag_id"], x)
        )

        segments = (
            {"segmap": frag_segmap, "target": tagged_fragments}
            | "GroupSegsWithMap" >> beam.CoGroupByKey()
            | "TagSegsWithsSegId" >> TagWithSegId()
            | "AddSegidKey" >> beam.Map(lambda x: (x["seg_id"], x))
            | "GroupBySegId" >> beam.GroupByKey()
            | "AddCumulativeData" >> CreateSegments()
            | "FilterSegsToDateRange" >> beam.Filter(self.in_date_range)
        )

        return {SEGMAP: segmap, SEGMENTS: segments}
