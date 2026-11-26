"""Transform that builds the segments from the whole history of fragments."""

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
    """Returns the UTC date of a timestamp in seconds."""
    return datetime_from_timestamp(ts).date()


def in_date_range(segment, start_date, end_date):
    """Returns True if the day of the segment is between the dates, both included."""
    return start_date <= timestamp_to_date(segment["timestamp"]) <= end_date


def strip_identity_and_destination(fragment):
    """Returns a copy of the fragment without its identities and destinations.

    They are not used to match the fragments.
    """
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
        """Initializes the transform."""
        self.existing_fragments = existing_fragments
        self.start_date = start_date
        self.end_date = end_date
        self.merge_params = merge_params

    def expand(self, new_fragments):
        """Returns the segment map and the segments of the new fragments.

        Args:
            new_fragments:
                The fragments of the date range.

        Returns:
            A dictionary with the segment map under ``SEGMAP`` and the segments under ``SEGMENTS``.
        """
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
