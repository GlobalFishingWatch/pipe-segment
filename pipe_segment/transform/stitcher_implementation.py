import bisect
from collections import Counter, namedtuple
import datetime as DT
import itertools as it
import logging
import six
import pytz

from gpsdio_segment.stitcher import Stitcher, Track, Signature
from gpsdio_segment.segment import Segment
from gpsdio_segment.segment import SegmentState

logger = logging.getLogger(__file__)
logger.setLevel(logging.WARNING)

BasicMessage = namedtuple("BasicMessage", 
    ['ssvid', 'timestamp', 'lat', 'lon', 'course', 'speed'])

AugSegmentState = namedtuple('AugSegmentState', 
    ['id', 'aug_id', 'ssvid', 'timestamp',
     'first_msg', 'last_msg', 'first_msg_of_day',  'last_msg_of_day', 
     'msg_count', 'noise', 'closed',
     'transponders', 'shipnames', 'callsigns', 'imos'])

PlaceHoldderSegmentState = namedtuple('PlaceHoldderSegmentState', 
        ['id', 'aug_id', 'timestamp', 'last_msg_of_day'])


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

    def _build_message(self, seg_record, prefix):
        return BasicMessage(
            ssvid = seg_record['ssvid'],
            timestamp = seg_record[prefix + 'timestamp'],
            lat = seg_record[prefix + 'lat'],
            lon = seg_record[prefix + 'lon'],
            course = seg_record[prefix + 'course'],
            speed = seg_record[prefix + 'speed']
        )

    def _as_imutable_sig(self, sig, scale):
        pairs = []
        for mapping in sig:
            pairs.append((mapping['value'], mapping['count'] / scale))
        return tuple(pairs)

    def _to_value_count(self, mapping):
        return [{'value' : k, 'count' : v} for (k, v) in mapping.items()]

    def _from_value_count(self, sequence):
        mapping = {}
        for x in sequence:
            mapping[x['value']] = x['count']
        return mapping

    def _segment_state(self, seg_record):
        first_msg = self._build_message(seg_record, 'first_msg_')
        last_msg = self._build_message(seg_record, 'last_msg_')
        first_msg_of_day = self._build_message(seg_record, 'first_msg_of_day_')
        last_msg_of_day = self._build_message(seg_record, 'last_msg_of_day_')
        count = seg_record['message_count']
        return AugSegmentState(
                            id = seg_record['seg_id'],
                            aug_id = Stitcher.aug_seg_id(seg_record),
                            timestamp = seg_record['timestamp'],
                            noise = False,
                            closed = seg_record['closed'],
                            ssvid = seg_record['ssvid'],
                            msg_count = seg_record['message_count'],
                            first_msg = first_msg,
                            last_msg = last_msg,
                            first_msg_of_day = first_msg_of_day,
                            last_msg_of_day = last_msg_of_day,
                            transponders = self._as_imutable_sig(seg_record['transponders'], count),
                            shipnames = self._as_imutable_sig(seg_record['shipnames'], count),
                            callsigns = self._as_imutable_sig(seg_record['callsigns'], count),
                            imos = self._as_imutable_sig(seg_record['imos'], count)
                            ) 


    def reconstitute_tracks(self, tracks, segments):
        # TODO: pull one extra day of segments and compute differential signatures
        # TODO: actually EWMA would be better but would have to happen in segmenter
        # TODO: so not right now. For now, just use average value
        # TODO: So, value / count
        seg_map = {x.aug_id : x for x in segments}
        for raw_track in tracks:
            reconst = []
            for aug_id in raw_track['seg_ids']:
                if aug_id not in seg_map:
                    id_, year, month, day = aug_id.rsplit('-', 3)
                    year, month, day = (int(x) for x in [year, month, day])
                    # Use the start of the day as the timestamp for missing tracks
                    timestamp = DT.datetime(year, month, day)
                    seg_map[aug_id] = PlaceHoldderSegmentState(id=id_, aug_id=aug_id, 
                                                timestamp=timestamp, last_msg_of_day=None)
                reconst.append(seg_map[aug_id])
            if isinstance(reconst[-1], PlaceHoldderSegmentState):
                last_msg = self._build_message(raw_track, 'last_msg_')
                reconst[-1] = reconst[-1]._replace(last_msg_of_day=last_msg)
            yield Track(id=raw_track['track_id'], prefix=reconst, segments=[], 
                           count=raw_track['count'], decayed_count=raw_track['decayed_count'], 
                           is_active=raw_track['is_active'],
                           signature=Signature(self._from_value_count(raw_track['transponders']), 
                                               self._from_value_count(raw_track['shipnames']),
                                               self._from_value_count(raw_track['callsigns']), 
                                               self._from_value_count(raw_track['imos'])))

    @staticmethod
    def _as_datetime(x):
        return DT.datetime.combine(x, DT.time()).replace(tzinfo=pytz.utc)

    def stitch(self, ssvid, tracks, seg_records):
        stitcher = Stitcher(**self.stitcher_params)
        start_datetime = self._as_datetime(self.start_date)

        aug_ids = set()
        segments = []
        for rcd in seg_records:
            seg = self._segment_state(rcd)
            # Ensure no duplicates
            if seg.aug_id not in aug_ids:
                segments.append(seg)
                aug_ids.add(seg.aug_id)

        initial_tracks = list(self.reconstitute_tracks(tracks, segments))
        # Assure segments sorted to simplify pruning
        segments= sorted(segments, key=lambda x: x.timestamp)

        # For now we are using pulling all segments in to reconstitute tracks
        # in the future it might makes sense performance wise to only pull in 
        # a finite time range of segments. To make that work, however, would
        # require that we embed the final segment record in the track.
        # track_sigs = stitcher.find_track_signatures(start_datetime, initial_tracks, segments)
        track_sigs = {} # TODO: we need to save track and then build them as we go.

        track_iter = stitcher.create_tracks(start_datetime, initial_tracks, track_sigs, segments)

        initial_track_ids = {track.id for track in initial_tracks}

        # Condense tracks
        raw_tracks = []
        for track, ndx in track_iter:

            all_segments = tuple(track.prefix) + tuple(track.segments)

            if track.id in initial_track_ids:
                emit_datetime = start_datetime
            else:
                emit_datetime = max(self._as_datetime(all_segments[0].timestamp.date()), start_datetime)

            while emit_datetime.date() <= self.end_date:
                segments = [x for x in all_segments if 
                                x.timestamp and x.timestamp.date() <= emit_datetime.date()]
                seg_ids = [x.aug_id for x in segments]
                last_msg = segments[-1].last_msg_of_day
                assert last_msg is not None
                if seg_ids:
                    yield {'ssvid' : ssvid, 
                           'track_id' : track.id, 
                           'index': ndx,
                           'timestamp' : self._as_datetime(emit_datetime), 
                           'seg_ids' : seg_ids, 
                           'count' : track.count, 
                           'decayed_count' : track.decayed_count,
                           'is_active' : track.is_active,
                           'last_msg_timestamp' : last_msg.timestamp, 
                           'last_msg_lat' : last_msg.lat, 
                           'last_msg_lon' : last_msg.lon,
                           'last_msg_course' : last_msg.course, 
                           'last_msg_speed' : last_msg.speed,
                           'transponders' : self._to_value_count(track.signature.transponders),
                           'shipnames' : self._to_value_count(track.signature.shipnames),
                           'callsigns' : self._to_value_count(track.signature.callsigns),
                           'imos' : self._to_value_count(track.signature.imos)
                           }
                emit_datetime += DT.timedelta(days=1)




        # while start_date <= self.end_date:
        #     end_date = start_date + DT.timedelta(days=self.look_ahead)
        #     # 
        #     pruned_tracks = []
        #     for track in raw_tracks:
        #         new_track = [x for x in track if x['timestamp'].date() < start_date]
        #         if new_track:
        #             pruned_tracks.append(new_track)
        #     # # Prune segments
        #     track_sigs = stitcher.find_track_signatures(start_date, pruned_tracks, segments)
        #     # for i, seg in enumerate(segments):
        #     #     if seg['timestamp'].date() >= start_date:
        #     #         break
        #     # segments = segments[i:]
        #     # Only keep segments up to lookahead forward
        #     first_seg_date = start_date - DT.timedelta(MAX_LOOKBACK)
        #     for i, seg in enumerate(segments):
        #         if seg['timestamp'].date() >= first_seg_date:
        #             break
        #     else:
        #         i += 1
        #     segments = segments[i:]
        #     for i, seg in enumerate(segments):
        #         if seg['timestamp'].date() > end_date:
        #             break
        #     else:
        #         i += 1
        #     #
        #     track_iter = stitcher.create_tracks(start_date, pruned_tracks, track_sigs, segments[:i])
        #     # Condense tracks
        #     raw_tracks = []
        #     for raw_track, ndx, status in track_iter:
        #         track = [x['aug_seg_id'] for x in raw_track]
        #         track_id = track[0]
        #         yield {'ssvid' : ssvid, 'track_id' : track_id, 'index': ndx,
        #                'timestamp' : self._as_datetime(start_date), 'seg_ids' : track}
        #         # TODO: status should be added to stored tracks and dealt with on input
        #         if status == 'active':
        #             raw_tracks.append(raw_track)
        #     start_date += DT.timedelta(days=1)

