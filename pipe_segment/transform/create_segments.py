import apache_beam as beam
from collections import defaultdict
from gpsdio_segment.matcher import Matcher
from datetime import date
import math

from ..tools import datetimeFromTimestamp


class CreateSegments(beam.PTransform):
    def __init__(self, args=None):
        if args is None:
            args = {}
        assert "lookback" not in args
        args["lookback"] = 0
        self.matcher = Matcher(**args)

    def frag2msg(self, frag, end):
        msg = {
            k: frag[f"{end}_msg_of_day_{k}"]
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
        # TODO: frags should already be sorted by day due to how they
        # TODO: are created, so this may not be necessary. Check.
        frags = sorted(frags, key=lambda x: x["timestamp"])
        current = []
        day = datetimeFromTimestamp(frags[0]["timestamp"]).date()
        for x in frags:
            new_day = datetimeFromTimestamp(x["timestamp"]).date()
            if new_day != day:
                assert len(current) > 0
                yield day, current
                current = []
                day = new_day
            current.append(x["frag_id"])
        assert len(current) > 0
        yield day, current

    def merge_fragments(self, item):
        key, frags = item
        frag_map = {x["frag_id"]: x for x in frags}
        open_segs = {}
        for day, daily_fids in self.frags_by_day(frags):

            scores = self.compute_scores(open_segs, daily_fids, frag_map)
            active = defaultdict(list)
            while scores and daily_fids:
                (sid, fid) = max(scores, key=lambda k: scores[k])
                if scores[sid, fid] == 0:
                    break
                active[sid].append((day, fid))
                daily_fids.remove(fid)
                scores.update(
                    self.compute_scores({sid: active[sid]}, daily_fids, frag_map)
                )
                for k in list(scores.keys()):
                    is_stale = k[1] == fid
                    if is_stale:
                        scores.pop(k)

            for sid, seg_fids in open_segs.items():
                for frag_day, fid in seg_fids:
                    assert isinstance(frag_day, date), (frag_day, sid, fid)
                    yield {"seg_id": sid, "date": frag_day, "frag_id": fid}

            open_segs = active
            for fid in daily_fids:
                sid = fid
                open_segs[sid] = [(day, fid)]

        for sid, seg_fids in open_segs.items():
            for frag_day, fid in seg_fids:
                assert isinstance(frag_day, date), (frag_day, sid, fid)
                yield {"seg_id": sid, "date": frag_day, "frag_id": fid}

    def expand(self, xs):
        return xs | beam.FlatMap(self.merge_fragments)