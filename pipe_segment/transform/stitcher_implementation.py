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
logger.setLevel(logging.WARNING)


class StitcherImplementation(object):

    def __init__(self,
                 start_date = None,
                 end_date = None,
                 look_ahead = 7,
                 stitcher_params = None):
        self.start_date = start_date
        self.end_date = end_date
        self.look_ahead = look_ahead
        self.stitcher_params = stitcher_params or {}

    def reconstitute_tracks(self, tracks, segments):
        seg_map = {Stitcher.aug_seg_id(x) : x for x in segments}
                        # if x['timestamp'].date() < self.start_date}
        for raw_track in tracks:
            reconst = []
            for x in raw_track['seg_ids']:
                reconst.append(seg_map[x])
            yield reconst

    @staticmethod
    def _as_datetime(x):
        return DT.datetime.combine(x, DT.time()).replace(tzinfo=pytz.utc)

    def stitch(self, ssvid, tracks, segments):
        stitcher = Stitcher(**self.stitcher_params)
        start_date = self.start_date

        raw_tracks = list(self.reconstitute_tracks(tracks, segments))

        # Assure segments sorted to simplify pruning
        segments= sorted(segments, key=lambda x:x['timestamp'])
        aug_seg_ids = set()
        for seg in segments:
            seg['aug_seg_id'] = stitcher.aug_seg_id(seg)
            if seg['aug_seg_id'] in aug_seg_ids:
                logger.warning('duplicate aug_seg_id: %s', seg['aug_seg_id'])
            
        # For now we are using pulling all segments in to reconstitute tracks
        # in the future it might makes sense performance wise to only pull in 
        # a finite time range of segments. To make that work, however, would
        # require that we embed the final segment record in the track.


        # Add track counts 

        while start_date <= self.end_date:
            end_date = start_date + DT.timedelta(days=self.look_ahead)
            # 
            pruned_tracks = []
            for track in raw_tracks:
                # TODO: use bisect to prune in log time
                new_track = [x for x in track if x['timestamp'].date() < start_date]
                if new_track:
                    pruned_tracks.append(new_track)
            # Look back 365 days eventually.
            # # Prune segments
            # # TODO: use bisect to prune in log time
            track_sigs = stitcher.find_track_signatures(start_date, pruned_tracks, segments)
            # for i, seg in enumerate(segments):
            #     if seg['timestamp'].date() >= start_date:
            #         break
            # segments = segments[i:]
            # Only keep segments up to lookahead forward
            for i, seg in enumerate(segments):
                if seg['timestamp'].date() > end_date:
                    break
            else:
                i += 1
            #
            track_iter = stitcher.create_tracks(start_date, pruned_tracks, track_sigs, segments[:i])
            # Condense tracks
            raw_tracks = []
            for raw_track, ndx in track_iter:
                track = [x['aug_seg_id'] for x in raw_track]
                track_id = track[0]
                yield {'ssvid' : ssvid, 'track_id' : track_id, 'index': ndx,
                       'timestamp' : self._as_datetime(start_date), 'seg_ids' : track}
                raw_tracks.append(raw_track)
            start_date += DT.timedelta(days=1)

