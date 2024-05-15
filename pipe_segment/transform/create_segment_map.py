from datetime import date

import apache_beam as beam
from gpsdio_segment.matcher import Matcher

from ..tools import datetimeFromTimestamp
from .util import by_day


class CreateSegmentMap(beam.PTransform):
    def __init__(self, args=None):
        if args is None:
            args = {}
        assert "lookback" not in args
        args["lookback"] = 0
        self.matcher = Matcher(**args)

    def frag2msg(self, frag, end):
        msg = {
            k: frag[f"{end}_msg_{k}"]
            for k in ["timestamp", "lon", "lat", "speed", "course"]
        }
        msg["timestamp"] = datetimeFromTimestamp(msg["timestamp"])
        return msg

    def compute_pair_score(self, frag0, frag1):
        msg0 = self.frag2msg(frag0, "last")
        msg1 = self.frag2msg(frag1, "first")
        hours = self.matcher.compute_msg_delta_hours(msg0, msg1)
        if not 0 < hours < 24:
            return 0.0
        penalized_hours = self.matcher.compute_penalized_hours(hours)
        discrepancy = self.matcher.compute_discrepancy(msg0, msg1, penalized_hours)
        return self.matcher.compute_metric(discrepancy, hours)

    def compute_scores(self, segs, frag_ids, frag_map):
        """
        Parameters
        ----------
        segs : dict mapping seg_id (str) to list of frag_ids (str)
        frag_ids : list of str
        frag_map : dict mapping str (frag_id) to dict
        """
        scores = {}
        for sid, dated_fids in segs.items():
            _, fid0 = dated_fids[-1]
            frag0 = frag_map[fid0]
            for fid in frag_ids:
                frag1 = frag_map[fid]
                scores[sid, fid] = self.compute_pair_score(frag0, frag1)
        return scores

    def frags_by_day(self, frags):
        for day, frags_by_day in by_day(frags):
            yield day, [x["frag_id"] for x in frags_by_day]

    def merge_fragments(self, item):
        key, frags = item
        frag_map = {x["frag_id"]: x for x in frags}
        open_segs = {}
        for day, daily_fids in self.frags_by_day(frags):

            # Compute how well fragments from today match earlier segments
            # A score of zero means do not match.
            scores = self.compute_scores(open_segs, daily_fids, frag_map)
            #
            active_segs = {}
            while scores:
                (sid, fid) = max(scores, key=lambda k: (scores[k], k))
                if scores[sid, fid] == 0:
                    break
                assert sid not in active_segs
                active_segs[sid] = [(day, fid)]
                daily_fids.remove(fid)
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
                for k in list(scores.keys()):
                    k_sid, k_fid = k
                    is_stale = (k_sid == sid) or (k_fid == fid)
                    if is_stale:
                        scores.pop(k)

            # Yield all segments where we match to an existing segment
            for sid, seg_fids in active_segs.items():
                assert len(seg_fids) == 1
                for frag_day, fid in seg_fids:
                    assert isinstance(frag_day, date), (frag_day, sid, fid)
                    assert frag_day == day
                    yield {"seg_id": sid, "date": frag_day, "frag_id": fid}

            # Create new segments where we do NOT match to a segment and
            # yield them
            open_segs = {}
            for fid in daily_fids:
                sid = fid
                open_segs[sid] = [(day, fid)]
                yield {"seg_id": sid, "date": day, "frag_id": fid}

            # Add any active segs to open_segs
            for k, v in active_segs.items():
                assert k not in open_segs, "open_segs and active shouldn't overlap"
                assert len(v) == 1, "should only be one item in active values"
                assert v[0][0] == day
                open_segs[k] = v

    def expand(self, xs):
        return xs | beam.FlatMap(self.merge_fragments)
