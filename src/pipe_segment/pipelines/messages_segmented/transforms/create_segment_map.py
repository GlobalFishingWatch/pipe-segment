"""Transform that matches the fragments of each vessel with its segments."""

from datetime import date

import apache_beam as beam
from gpsdio_segment.matcher import Matcher
from typing import Iterable, Generator, Any, Optional, List, Tuple, Set, Dict

from pipe_segment.tools import datetime_from_timestamp
from .segment_state import SegmentState
from .util import by_day


def get_next(
    ordered: List[Tuple[float, str, str]], stale_keys: Set[str]
) -> Optional[Tuple[Any, str, str]]:
    """return the next item in the queue, skipping items with stale keys"""
    while ordered:
        item = ordered.pop()
        _, id1, id2 = item
        if id1 not in stale_keys and id2 not in stale_keys:
            return item
    return None


class CreateSegmentMap(beam.PTransform):
    """Matches the fragments of each vessel with segments, with one row per fragment and day.

    The new fragments of each day are scored against the segments that were open the day before,
    with the matcher of gpsdio-segment. Each one joins the segment with the best score, or starts
    a new segment if it does not match any.

    Args:
        args:
            Parameters of the matcher. Its lookback is always 0, so it cannot be given.
    """

    def __init__(self, args=None):
        """Initializes the transform."""
        if args is None:
            args = {}

        assert "lookback" not in args
        args["lookback"] = 0
        self.matcher = Matcher(**args)

    def frag2msg(self, frag, end):
        """Returns a message with the position and the time of one end of a fragment.

        Args:
            frag:
                The fragment.

            end:
                The end of the fragment to take: ``"first"`` or ``"last"``.

        Returns:
            The message, with its timestamp as a datetime, and its lon, lat, speed and course.
        """
        msg = {
            k: frag[f"{end}_msg_{k}"]
            for k in ["timestamp", "lon", "lat", "speed", "course"]
        }
        msg["timestamp"] = datetime_from_timestamp(msg["timestamp"])
        return msg

    def compute_pair_score(self, frag0, frag1):
        """Returns how well the start of a fragment continues the end of another one.

        Args:
            frag0:
                The fragment that ends first. Its last message is used.

            frag1:
                The fragment that starts later. Its first message is used.

        Returns:
            The score of the match, where higher is better. It is 0.0 if the time between the two
            messages is not between 0 and 24 hours.
        """
        msg0 = self.frag2msg(frag0, "last")
        msg1 = self.frag2msg(frag1, "first")
        hours = self.matcher.compute_msg_delta_hours(msg0, msg1)
        if not 0 < hours < 24:
            return 0.0

        penalized_hours = self.matcher.compute_penalized_hours(hours)
        discrepancy = self.matcher.compute_discrepancy(msg0, msg1, penalized_hours)

        return self.matcher.compute_metric(discrepancy, hours)

    def compute_ordered_scores(
        self,
        existing_fragments: Iterable[Tuple[str, str]],
        new_fragments: Iterable[str],
        frag_map: Dict[str, dict],
    ) -> List[Tuple[float, str, str]]:
        """
        Args:
            existing_fragments:
                Tuples of the seg_id and the frag_id of the last fragment of each open segment.

            new_fragments:
                The frag_ids of the new fragments to match.

            frag_map:
                Mapping of frag_id to fragments.

        Returns:
            Sorted list of (score, segment_id, frag_id). Note highest scores
            are last for easy access in order of highest score
        """
        scores = []
        for seg_id, frag_id_0 in existing_fragments:
            frag0 = frag_map[frag_id_0]
            for frag_id_1 in new_fragments:
                frag1 = frag_map[frag_id_1]
                score = self.compute_pair_score(frag0, frag1)
                scores.append((score, seg_id, frag_id_1))
        scores.sort()  # This puts highest scores last
        return scores

    def frags_by_day(
        self, frags: Iterable[dict]
    ) -> Generator[Tuple[date, Set[str]], None, None]:
        """Yields the ids of the fragments of each day, in date order.

        Args:
            frags:
                The fragments.

        Yields:
            Tuples of the day and the set of the frag_ids of its fragments.
        """
        for day, frags_by_day in by_day(frags):
            yield day, {x["frag_id"] for x in frags_by_day}

    def merge_fragments(
        self, ssvid_frags: Tuple[Any, Iterable[Dict]]
    ) -> Generator[Dict, None, None]:
        """Matches the fragments of a vessel, starting with no open segments.

        Args:
            ssvid_frags:
                Tuple of the ssvid and its fragments.

        Yields:
            One row per fragment and day, with its ssvid, date, seg_id and frag_id.
        """
        ssvid, frags = ssvid_frags
        yield from self.merge_fragments_with_state(ssvid, frags)

    def merge_fragments_with_state(
        self,
        ssvid: Any,
        frags: Iterable[Dict],
        prior_states: Iterable[SegmentState] = (),
    ) -> Generator[Dict, None, None]:
        """Matches the fragments of a vessel with the segments open before the first one.

        Args:
            ssvid:
                The vessel.

            frags:
                The fragments to match, of one or more days.

            prior_states:
                The segments open at the end of the day before the first fragment.
                When empty, the first day starts with no open segments.
        """
        frags = list(frags)
        frag_map = {x["frag_id"]: x for x in frags}
        frag_map.update({state.frag_id: state.as_last_fragment() for state in prior_states})
        open_segs: Dict[str, str] = {state.seg_id: state.frag_id for state in prior_states}
        new_fragments: Set[str]
        for day, new_fragments in self.frags_by_day(frags):
            # Compute how well fragments from today match earlier segments
            # A score of zero means do not match.
            ordered_scores = self.compute_ordered_scores(
                open_segs.items(), new_fragments, frag_map
            )
            active_segs = {}
            stale_keys: Set[str] = set()
            while (item := get_next(ordered_scores, stale_keys)) is not None:
                score, seg_id, frag_id = item
                if score == 0:
                    break
                assert seg_id not in active_segs
                active_segs[seg_id] = frag_id
                new_fragments.remove(frag_id)
                # We used to update scores as we added fragments,
                # which allows joining within a single day[*]. While
                # this still seems like a good idea in principle,
                # it makes keeping consistency between daily and
                # longer modes too hard, (unless we delay segments
                # by a day.) So for now, don't recompute and mark
                # both the fragment and segment as stale so they
                # only get joined once per day.
                #
                # [*] This happens by two fragments from the next day
                # joining the same segment from the previous day.
                stale_keys.add(seg_id)
                stale_keys.add(frag_id)

            # Yield all segments where we match to an existing segment
            for seg_id, frag_id in active_segs.items():
                yield {
                    "ssvid": ssvid,
                    "date": day,
                    "seg_id": seg_id,
                    "frag_id": frag_id,
                }

            # Create new segments where we do NOT match to a segment and
            # yield them
            open_segs = {}
            for frag_id in new_fragments:
                # The new segment takes its ID from the first frag_id
                seg_id = frag_id
                open_segs[seg_id] = frag_id
                yield {
                    "ssvid": ssvid,
                    "date": day,
                    "seg_id": seg_id,
                    "frag_id": frag_id,
                }

            # Add any active segs to open_segs
            for seg_id, frag_id in active_segs.items():
                assert seg_id not in open_segs, "open_segs and active shouldn't overlap"
                open_segs[seg_id] = frag_id

    def expand(self, xs):
        """Matches the fragments of each vessel, grouped by ssvid."""
        return xs | beam.FlatMap(self.merge_fragments)
