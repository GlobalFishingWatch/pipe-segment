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
     'transponders', 'shipnames', 'callsigns', 'imos', 'daily_msg_count'])

PlaceHolderSegmentState = namedtuple('PlaceHolderSegmentState', 
        ['id', 'aug_id', 'timestamp', 'last_msg_of_day'])


class StitcherImplementation(object):

    def __init__(self,
                 start_date = None,
                 end_date = None,
                 stitcher_params = None):
        self.start_date = start_date
        self.end_date = end_date
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

    def _as_imutable_sig(self, sig):
        pairs = []
        for mapping in sig:
            pairs.append((mapping['value'], mapping['count']))
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
                            transponders = self._as_imutable_sig(seg_record['transponders']),
                            shipnames = self._as_imutable_sig(seg_record['shipnames']),
                            callsigns = self._as_imutable_sig(seg_record['callsigns']),
                            imos = self._as_imutable_sig(seg_record['imos']),
                            daily_msg_count = None,
                            ) 


    def reconstitute_tracks(self, tracks):
        for raw_track in tracks:
            yield Track(id=raw_track['track_id'], 
                        seg_ids=raw_track['seg_ids'], 
                        last_msg=self._build_message(raw_track, 'last_msg_'),
                        count=raw_track['count'], 
                        decayed_count=raw_track['decayed_count'], 
                        is_active=raw_track['is_active'],
                        signature=Signature(
                            transponders=self._from_value_count(raw_track['transponders']), 
                            shipnames=self._from_value_count(raw_track['shipnames']),
                            callsigns=self._from_value_count(raw_track['callsigns']), 
                            imos=self._from_value_count(raw_track['imos'])),
                        parent_track=None
                    )

    @staticmethod
    def _as_datetime(x):
        return DT.datetime.combine(x, DT.time(tzinfo=pytz.utc))

    def prepare_segments(self, segments):
        segments= sorted(segments, key=lambda x: (x.id, x.timestamp))
        start_datetime = self._as_datetime(self.start_date)
        current_id = None
        for seg in segments:
            if seg.id != current_id:
                last_count = 0
                current_id = seg.id
            seg = seg._replace(daily_msg_count=seg.msg_count - last_count)
            last_count = seg.msg_count
            if seg.timestamp >= start_datetime:
                yield seg


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

        initial_tracks = list(self.reconstitute_tracks(tracks))
        segments = list(self.prepare_segments(segments))
        initial_track_ids = {track.id for track in initial_tracks}

        raw_tracks = []
        for base_track in stitcher.create_tracks(start_datetime, initial_tracks, segments):
            tracks_by_date = {}
            track = base_track
            while True:
                date = track.last_msg.timestamp.date()
                if date not in tracks_by_date or track.last_msg.timestamp > tracks_by_date[date].last_msg.timestamp:
                    tracks_by_date[date] = track

                if track.parent_track is None:
                    break
                track = track.parent_track
            if (self.start_date not in tracks_by_date and 
                    base_track.id in initial_track_ids):
                # If we haven't set the track for start date, use oldest track
                # Oldest this can be is the input track, since we don't have any
                # tracks previous to that.
                tracks_by_date[self.start_date] = track


            if base_track.id in initial_track_ids:
                emit_datetime = start_datetime
            else:
                emit_datetime = max(self._as_datetime(min(tracks_by_date)), start_datetime)

            track = None

            while emit_datetime.date() <= self.end_date:
                track = tracks_by_date.get(emit_datetime.date(), track)

                sig = track.signature
                last_msg = track.last_msg

                yield {'ssvid' : ssvid, 
                       'track_id' : track.id, 
                       'index': None, 
                       'timestamp' : emit_datetime, 
                       'seg_ids' : track.seg_ids, 
                       'count' : track.count, 
                       'decayed_count' : track.decayed_count,
                       'is_active' : track.is_active,
                       'last_msg_timestamp' : last_msg.timestamp, 
                       'last_msg_lat' : last_msg.lat, 
                       'last_msg_lon' : last_msg.lon,
                       'last_msg_course' : last_msg.course, 
                       'last_msg_speed' : last_msg.speed,
                       'transponders' : self._to_value_count(sig.transponders),
                       'shipnames' : self._to_value_count(sig.shipnames),
                       'callsigns' : self._to_value_count(sig.callsigns),
                       'imos' : self._to_value_count(sig.imos),
                       }
                emit_datetime += DT.timedelta(days=1)


