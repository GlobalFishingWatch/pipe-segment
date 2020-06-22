import bisect
from collections import Counter, namedtuple
import datetime as DT
import itertools as it
import re
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
     'transponders', 'shipnames', 'callsigns', 'imos', 
     'destinations', 'lengths', 'widths',
     'daily_msg_count'])

PlaceHolderSegmentState = namedtuple('PlaceHolderSegmentState', 
        ['id', 'aug_id', 'timestamp', 'last_msg_of_day'])


class StitcherImplementation(object):

    def __init__(self,
                 start_date,
                 end_date,
                 look_ahead,
                 look_back,
                 stitcher_params = None):
        self.look_ahead = look_ahead
        self.look_back =  look_back
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
        return AugSegmentState(
                            id = seg_record['seg_id'],
                            aug_id = Stitcher.aug_seg_id(seg_record),
                            timestamp = seg_record['timestamp'],
                            noise = False,
                            closed = seg_record['closed'],
                            ssvid = seg_record['ssvid'],
                            msg_count = seg_record['message_count'],
                            daily_msg_count = seg_record['daily_message_count'],
                            first_msg = first_msg,
                            last_msg = last_msg,
                            first_msg_of_day = first_msg_of_day,
                            last_msg_of_day = last_msg_of_day,
                            transponders = self._as_imutable_sig(seg_record['transponders']),
                            shipnames = self._as_imutable_sig(seg_record['shipnames']),
                            callsigns = self._as_imutable_sig(seg_record['callsigns']),
                            destinations = self._as_imutable_sig(seg_record['destinations']),
                            lengths = self._as_imutable_sig(seg_record['lengths']),
                            widths = self._as_imutable_sig(seg_record['widths']),
                            imos = self._as_imutable_sig(seg_record['imos']),
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
                            destinations=self._from_value_count(raw_track['destinations']), 
                            lengths=self._from_value_count(raw_track['lengths']), 
                            widths=self._from_value_count(raw_track['widths']), 
                            imos=self._from_value_count(raw_track['imos'])),
                        parent_track=None
                    )

    @staticmethod
    def _as_datetime(x):
        return DT.datetime.combine(x, DT.time(tzinfo=pytz.utc))

    @staticmethod
    def remove_extra_segments(segments):
        # There is a bug in segments that I haven't tracked down yet
        # that results in occasional multiple segments per day. Filter out 
        seen = set()
        for seg in sorted(segments, key=lambda x: (x.aug_id, x.msg_count), reverse=True):
            key = seg.aug_id
            if key not in seen:
                yield seg
            seen.add(key)

    def _consolidate_signatures(self, segments):
        sig = {name : {} for name in Signature._fields}
        for seg in segments:
            for name in Signature._fields:
                for v, c in getattr(seg, name):
                    # Remove battery suffixes from gear / buoys
                    v = re.sub(r'(^|[ @_-])1?\d(\.?\d)?V$', '', v)
                    sig[name][v] = sig[name].get(v, 0) + c
        return {k : tuple(v.items()) for (k, v) in sig.items()}


    def prepare_segments(self, segments):
        deduped_segments = sorted(self.remove_extra_segments(segments), key=lambda x: (x.id, x.timestamp))
        start_datetime = self._as_datetime(self.start_date)
        current_id = None
        for seg in deduped_segments:
            if seg.id != current_id:
                working_segs = []
                current_id = seg.id
            # We collect signature information from the past `lookback` days
            # to stabilize the signature information a bit.
            working_segs.append(seg)
            while len(working_segs) > self.look_back:
                working_segs.pop(0)
            seg = seg._replace(**self._consolidate_signatures(working_segs))
            if seg.timestamp >= start_datetime:
                yield seg


    def stitch(self, ssvid, tracks, seg_records):
        stitcher = Stitcher(**self.stitcher_params)
        start_datetime = self._as_datetime(self.start_date)

        initial_tracks = list(self.reconstitute_tracks(tracks))
        segments = list(self.prepare_segments([self._segment_state(rcd) for rcd in seg_records]))
        end_datetime = self._as_datetime(self.end_date)

        emit_datetime = start_datetime
        while emit_datetime <= end_datetime:
            current_segments = [x for x in segments 
                                    if emit_datetime <= self._as_datetime(x.timestamp.date()) <= emit_datetime + DT.timedelta(days=self.look_ahead - 1)] 


            new_tracks = []
            for base_track in stitcher.create_tracks(emit_datetime, initial_tracks, current_segments):
                track = base_track
                while True:
                    if self._as_datetime(track.last_msg.timestamp.date()) <= emit_datetime:
                        break
                    if track.parent_track is None:
                        track = None
                        break
                    track = track.parent_track

                if track is None:
                    continue

                new_tracks.append(track)

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
                       'destinations' : self._to_value_count(sig.destinations),
                       'lengths' : self._to_value_count(sig.lengths),
                       'widths' : self._to_value_count(sig.widths),
                       'imos' : self._to_value_count(sig.imos),
                       }

            initial_tracks = new_tracks
            emit_datetime += DT.timedelta(days=1)





