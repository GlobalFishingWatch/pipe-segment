from collections import defaultdict
from typing import Any, Dict, Generator, Tuple

import apache_beam as beam

from apache_beam.pvalue import TaggedOutput

from pipe_segment.transform.create_segment_map import CreateSegmentMap
from pipe_segment.transform.create_segments import CreateSegments
from pipe_segment.transform.full_history_segments import SEGMAP, SEGMENTS
from pipe_segment.transform.read_prior_segments import ReadPriorSegments
from pipe_segment.transform.segment_state import SegmentState


class IncrementalSegments(beam.PTransform):
    """Builds the segment map and the segments starting from the segments of the day before.

    Matching only looks at the segments open the day before, and the cumulative values are
    already in the segments of that day, so the fragments written before the date range
    are not needed. Each vessel is processed in one step: it is matched with its open
    segments and its segments-days are computed right after.

    Args:
        prior_segments: transform that reads the state of the segments before the date range.
        merge_params: parameters of the matcher.
    """

    def __init__(self, prior_segments: ReadPriorSegments, merge_params: Dict):
        self.prior_segments = prior_segments
        self.segment_map = CreateSegmentMap(merge_params)
        self.segments = CreateSegments()

    def verify(self):
        self.prior_segments.verify()

    def update(self, item: Tuple[Any, Dict]) -> Generator:
        """Matches the new fragments of a vessel and computes its segments-days.

        Args:
            item: the ssvid and the new fragments and prior state of the vessel.
        """
        ssvid, grouped = item
        fragments = list(grouped["fragments"])
        if not fragments:
            return

        states = [SegmentState.from_row(row) for row in grouped["state"]]
        segmap = list(self.segment_map.merge_fragments_with_state(ssvid, fragments, states))
        for row in segmap:
            yield TaggedOutput(SEGMAP, row)

        seg_id_by_frag_id = {row["frag_id"]: row["seg_id"] for row in segmap}
        fragments_by_seg_id = defaultdict(list)
        for fragment in fragments:
            fragment = fragment.copy()
            fragment["seg_id"] = seg_id_by_frag_id[fragment["frag_id"]]
            fragments_by_seg_id[fragment["seg_id"]].append(fragment)

        prior_by_seg_id = {state.seg_id: state for state in states}
        for seg_id, seg_fragments in fragments_by_seg_id.items():
            yield from self.segments.update_msgs(
                (seg_id, seg_fragments), prior_by_seg_id.get(seg_id)
            )

    def expand(self, new_fragments):
        prior_state = new_fragments.pipeline | self.prior_segments
        keyed_fragments = new_fragments | "AddSsvidKey" >> beam.Map(lambda x: (x["ssvid"], x))

        outputs = (
            {"fragments": keyed_fragments, "state": prior_state}
            | "GroupBySsvid" >> beam.CoGroupByKey()
            | "UpdateSegments" >> beam.FlatMap(self.update).with_outputs(SEGMAP, main=SEGMENTS)
        )

        return {SEGMAP: outputs[SEGMAP], SEGMENTS: outputs[SEGMENTS]}
