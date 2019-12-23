import bisect
from collections import Counter
import datetime as DT
import itertools as it
import logging
import six
import pytz

from gpsdio_segment.stitcher import Stitcher
from gpsdio_segment.segment import Segment

logger = logging.getLogger(__file__)
logger.setLevel(logging.INFO)


class StitcherImplementation(object):

    def __init__(self,
                 start_date = None,
                 end_date = None,
                 stitcher_params = None):
        self.start_date = start_date
        self.end_date = end_date
        self.stitcher_params = stitcher_params or {}

    @staticmethod
    def reconstitute_tracks(tracks, segments):
        seg_map = {Stitcher.aug_seg_id(x) : x for x in segments}
        for raw_track in tracks:
            reconst = []
            for x in raw_track['seg_ids']:
                if x in seg_map:
                    reconst.append(seg_map[x])
                else:
                    print('missing seg_id', x)
            yield reconst

    @staticmethod
    def _as_datetime(x):
        return DT.datetime.combine(x, DT.time()).replace(tzinfo=pytz.utc)

    def stitch(self, ssvid, tracks, segments):
        stitcher = Stitcher(**self.stitcher_params)
        start_date = self.start_date

        raw_tracks = list(self.reconstitute_tracks(tracks, segments))
            
        # For now we are using pulling all segments in to reconstitute tracks
        # in the future it might makes sense performance wise to only pull in 
        # a finite time range of segments. To make that work, however, would
        # require that we embed the final segment record in the track.

        while start_date <= self.end_date:
            pruned_tracks = []
            for track in raw_tracks:
                new_track = [x for x in track if x['timestamp'].date() < start_date]
                if new_track:
                    pruned_tracks.append(new_track)
            track_sigs = stitcher.find_track_signatures(start_date, pruned_tracks, segments)
            pruned_segs = [x for x in segments if x['timestamp'].date() >= start_date]
            raw_tracks = stitcher.create_tracks(start_date, pruned_tracks, track_sigs, pruned_segs)
            # Condense tracks
            for raw_track in raw_tracks:
                track = [stitcher.aug_seg_id(x) for x in raw_track]
                track_id = track[0]
                yield {'ssvid' : ssvid, 'track_id' : track_id, 
                       'timestamp' : self._as_datetime(start_date), 'seg_ids' : track}
            start_date += DT.timedelta(days=1)

