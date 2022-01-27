import apache_beam as beam
from collections import defaultdict
from gpsdio_segment.matcher import Matcher
import math

from ..timestamp import datetimeFromTimestamp


class CreateSegments(beam.PTransform):
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
        # TODO: clean this up, add support to Matcher.
        msg0 = self.frag2msg(frag0, "last")
        msg1 = self.frag2msg(frag1, "first")
        hours = self.matcher.compute_msg_delta_hours(msg0, msg1)
        if hours <= 0:
            return 0
        # Shorten the hours traveled relative to the length of travel
        # as boats tend to go straight for shorter distances, but at
        # longer distances, they may not go as straight or travel
        # the entire time
        penalized_hours = hours / (
            1 + (hours / self.matcher.penalty_hours) ** (1 - self.matcher.hours_exp)
        )
        discrepancy = self.matcher.compute_discrepancy(msg0, msg1, penalized_hours)
        padded_hours = math.hypot(hours, self.matcher.buffer_hours)
        max_allowed_discrepancy = padded_hours * self.matcher.max_knots
        if discrepancy > max_allowed_discrepancy:
            return 0
        alpha = (
            self.matcher._discrepancy_alpha_0 * discrepancy / max_allowed_discrepancy
        )
        metric = math.exp(-(alpha ** 2)) / padded_hours  # ** 2
        # Not worrying about transponder mismatch for now, would have to characterize how
        # uniform transmission is in each chunk -- probably not worth it.
        return metric

    def compute_scores(self, segs, frag_ids, frag_map):
        """
        Parameters
        ----------
        segs : dict mapping seg_id (str) to list of frag_ids (str)
        frag_ids : list of str
        frag_map : dict mapping str (frag_id) to dict
        """
        scores = {}
        for sid in segs:
            frag0 = frag_map[segs[sid][-1]]
            for fid in frag_ids:
                frag1 = frag_map[fid]
                scores[sid, fid] = self.compute_pair_score(frag0, frag1)
        return scores

    def frags_by_day(self, frags):
        frags = sorted(frags, key=lambda x: x["timestamp"])
        current = []
        day = datetimeFromTimestamp(frags[0]["timestamp"])
        for x in frags:
            new_day = datetimeFromTimestamp(x["timestamp"])
            if new_day != day:
                assert len(current) > 0
                yield current
                current = []
                day = new_day
            current.append(x["frag_id"])
        assert len(current) > 0
        yield current

    def merge_fragments(self, item):
        key, frags = item
        frag_map = {x["frag_id"]: x for x in frags}
        open_segs = {}
        for daily_fids in self.frags_by_day(frags):

            scores = self.compute_scores(open_segs, daily_fids, frag_map)
            active = defaultdict(list)
            while scores and daily_fids:
                (sid, fid) = max(scores, key=lambda k: scores[k])
                if scores[sid, fid] == 0:
                    break
                active[sid].append(fid)
                daily_fids.remove(fid)
                scores.update(
                    self.compute_scores({sid: active[sid]}, daily_fids, frag_map)
                )
                for k in list(scores.keys()):
                    is_stale = k[1] == fid
                    if is_stale:
                        scores.pop(k)

            for sid, seg_fids in open_segs.items():
                for fid in seg_fids:
                    yield {"seg_id": sid, "frag_id": fid}

            open_segs = active
            for fid in daily_fids:
                sid = fid
                open_segs[sid] = [fid]

        for sid, seg_fids in active.items():
            for fid in seg_fids:
                yield {"seg_id": sid, "frag_id": fid}

    def expand(self, xs):
        return xs | beam.FlatMap(self.merge_fragments)
