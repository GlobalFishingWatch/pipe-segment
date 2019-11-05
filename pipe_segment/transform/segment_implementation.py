import bisect
import datetime as dt
import itertools as it
import logging
import six
import pytz

from gpsdio_segment.core import Segmentizer
from gpsdio_segment.segment import SegmentState

logger = logging.getLogger(__file__)
logger.setLevel(logging.INFO)


class SegmentImplementation(object):

    OUTPUT_TAG_MESSAGES = 'messages'
    OUTPUT_TAG_SEGMENTS = 'segments'

    def __init__(self,
                 start_date = None,
                 end_date = None,
                 look_ahead = 0,
                 segmenter_params = None):
        self.look_ahead = look_ahead
        self.start_date = start_date
        self.end_date = end_date
        self.segmenter_params = segmenter_params or {}

    def _segment_record(self, seg_state, timestamp):
        first_msg = seg_state.first_msg
        last_msg = seg_state.last_msg
        return dict(
            seg_id=seg_state.id,
            ssvid=seg_state.ssvid,
            closed=seg_state.closed,
            message_count=seg_state.msg_count,
            first_timestamp=first_msg['timestamp'],
            first_lat=first_msg['lat'],
            first_lon=first_msg['lon'],
            first_course=first_msg['course'],
            first_speed=first_msg['speed'],
            last_timestamp=last_msg['timestamp'],
            last_lat=last_msg['lat'],
            last_lon=last_msg['lon'],
            last_course=last_msg['course'],
            last_speed=last_msg['speed'],
            timestamp=timestamp
        )

    def _segment_state(self, seg_record):
        first_msg = {
                'ssvid': seg_record['ssvid'],
                'timestamp': seg_record['first_timestamp'],
                'lat': seg_record['first_lat'],
                'lon': seg_record['first_lon'],
                'course' : seg_record['first_course'],
                'speed' : seg_record['first_speed']
            }
        last_msg = {
                'ssvid': seg_record['ssvid'],
                'timestamp': seg_record['last_timestamp'],
                'lat': seg_record['last_lat'],
                'lon': seg_record['last_lon'],
                'course' : seg_record['last_course'],
                'speed' : seg_record['last_speed']
            }
        return SegmentState(id = seg_record['seg_id'],
                            noise = False,
                            closed = seg_record['closed'],
                            ssvid = seg_record['ssvid'],
                            msg_count = seg_record['message_count'],
                            first_msg = first_msg,
                            last_msg = last_msg)


    @staticmethod
    def _as_datetime(x):
        return dt.datetime.combine(x, dt.time()).replace(tzinfo=pytz.utc)

    def _windowed_groups(self, messages, look_ahead):
        """yield overlapping sequences of n_days shifted by one day each time"""
        if not isinstance(messages, list):
            messages = list(messages)
        if not messages:
            return
        dates = [self._as_datetime(msg['timestamp'].date()) for msg in messages]
        first_date = self._as_datetime(self.start_date) if self.start_date else dates[0]
        final_first_date = self._as_datetime(self.end_date) if self.end_date else dates[-1]
        while first_date <= final_first_date:
            last_date = first_date + dt.timedelta(days=look_ahead)
            i0 = bisect.bisect_left(dates, first_date)
            i1 = bisect.bisect_right(dates, last_date)
            yield first_date, messages[i0:i1]
            first_date += dt.timedelta(days=1)

    def _convert_messages_out(self, msg, seg_id):
        msg = msg.copy()
        msg['seg_id'] = seg_id
        # Convert from {k : v} to [{k  : v}]
        for k1 in ['shipnames', 'callsigns', 'imos']:
            if k1 in msg:
                msg[k1] = [{k1[:-1] : k, 'cnt' : v} for (k, v) in msg[k1].items()]
            else:
                msg[k1] = None
        return msg

    def _extract_startup_info(self, msgs):
        msgids = {}
        locations = {}
        if not msgs:
            return msgs, msgids, locations
        start = self.start_date or msgs[0]['timestamp'].date()
        msgs = list(msgs)
        for i, msg in enumerate(msgs):
            ts = msg['timestamp']
            if ts.date() >= start:
                break
            msgids[msg['msgid']] = ts
            if msg['speed'] > 0:
                loc = Segmentizer.normalize_location(
                        *Segmentizer.extract_location(msg))
                locations[loc] = ts
        return msgs[i:], msgids, locations


    def segment(self, messages, seg_records):
        messages, msgids, locations = self._extract_startup_info(messages)
        for timestamp, grouped_msgs in self._windowed_groups(messages, self.look_ahead):
            date = timestamp.date()
            seg_states = [self._segment_state(rcd) for rcd in seg_records]
            segments = Segmentizer.from_seg_states(seg_states, grouped_msgs, 
                        prev_msgids=msgids,
                        prev_locations=locations,
                        prev_info=set(), # XXX Not obvious how to combine with lookahead
                        **self.segmenter_params)
            seg_records = []
            for seg in segments:
                if not seg.noise:
                    is_closed = seg.closed
                    if seg.last_msg['timestamp'].date() > date:
                        is_closed = False
                    seg.msgs = [x for x in seg.msgs if x['timestamp'].date() <= date]
                    if seg.msgs or seg.prev_state:
                        logger.debug('Segmenting key %r yielding segment %s containing %s messages ' % (seg.ssvid, seg.id, len(seg)))
                        rcd = self._segment_record(seg.state, timestamp)
                        rcd['closed'] = is_closed
                        output_rcd = rcd.copy()
                        output_rcd['timestamp'] = timestamp
                        yield (self.OUTPUT_TAG_SEGMENTS, output_rcd)
                        if not is_closed:
                            seg_records.append(rcd)
                else:
                    seg.msgs = [x for x in seg.msgs if x['timestamp'].date() <= date]
                seg_id = None if seg.noise else seg.id
                for msg in seg:
                    msg = self._convert_messages_out(msg, seg_id)
                    yield (self.OUTPUT_TAG_MESSAGES, msg)
            msgids = {mid : ts for (mid, ts) in segments.cur_msgids.items() 
                            if ts.date() <= timestamp.date()}
            locations = {loc : ts for (loc, ts) in segments.cur_locations.items()
                            if ts.date() <= timestamp.date()}

