import datetime as dt

from typing import Dict

import apache_beam as beam

from pipe_segment.tools import datetime_from_timestamp

from .create_segment_map import CreateSegmentMap
from .create_segments import CreateSegments
from .tag_with_seg_id import TagWithSegId


SEGMAP = "segmap"
SEGMENTS = "segments"


def timestamp_to_date(ts: float) -> dt.date:
    return datetime_from_timestamp(ts).date()


def in_date_range(segment, start_date, end_date):
    return start_date <= timestamp_to_date(segment["timestamp"]) <= end_date


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
        existing_fragments: transform that reads the fragments written before the date range.
        start_date: first day to write segments for.
        end_date: last day to write segments for.
        merge_params: parameters of the matcher.
    """

    def __init__(
        self,
        existing_fragments: beam.PTransform,
        start_date: dt.date,
        end_date: dt.date,
        merge_params: Dict,
    ):
        self.existing_fragments = existing_fragments
        self.start_date = start_date
        self.end_date = end_date
        self.merge_params = merge_params

    def expand(self, new_fragments):
        existing_fragments = new_fragments.pipeline | self.existing_fragments

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
            | "FilterSegsToDateRange"
            >> beam.Filter(in_date_range, self.start_date, self.end_date)
        )

        return {SEGMAP: segmap, SEGMENTS: segments}
