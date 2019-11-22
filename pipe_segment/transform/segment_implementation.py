import bisect
from collections import Counter
import datetime as dt
import itertools as it
import logging
import six
import pytz

from pipe_segment.stats import MessageStats

from gpsdio_segment.core import Segmentizer, INFO_PING_INTERVAL_MINS
from gpsdio_segment.segment import SegmentState

logger = logging.getLogger(__file__)
logger.setLevel(logging.INFO)


class SegmentImplementation(object):

    OUTPUT_TAG_MESSAGES = 'messages'
    OUTPUT_TAG_SEGMENTS_V1 = 'segments_v1'
    OUTPUT_TAG_SEGMENTS = 'segments'

    DEFAULT_STATS_FIELDS = [('lat', MessageStats.NUMERIC_STATS),
                            ('lon', MessageStats.NUMERIC_STATS),
                            ('timestamp', MessageStats.NUMERIC_STATS),
                            ('shipname', MessageStats.FREQUENCY_STATS),
                            ('imo', MessageStats.FREQUENCY_STATS),
                            ('callsign', MessageStats.FREQUENCY_STATS)]

    def __init__(self,
                 start_date = None,
                 end_date = None,
                 look_ahead = 0,
                 stats_fields = None,
                 segmenter_params = None):
        self.look_ahead = look_ahead
        self.start_date = start_date
        self.end_date = end_date
        self.stats_fields = self.DEFAULT_STATS_FIELDS if (stats_fields is None) else stats_fields
        self.segmenter_params = segmenter_params or {}

    @staticmethod
    def stat_output_field_name(field_name, stat_name):
        return '%s_%s' % (field_name, stat_name)

    def _segment_record(self, seg_state, messages, timestamp, signature):

        stats_numeric_fields = [f for f, stats in self.stats_fields if set(stats) & set (MessageStats.NUMERIC_STATS)]
        stats_frequency_fields = [f for f, stats in self.stats_fields if set(stats) & set (MessageStats.FREQUENCY_STATS)]
        safe_msgs = []
        has_timestamp = 'timestamp' in stats_numeric_fields
        stats_numeric_fields = [x for x in stats_numeric_fields if x != 'timestamp']
        ms = MessageStats(messages, stats_numeric_fields, stats_frequency_fields)

        first_msg = seg_state.first_msg
        last_msg = seg_state.last_msg
        def sig2rcd(name):
            items = []
            assert name.endswith('s')
            for k, v in signature.get(name, {}).items():
                items.append({'value' : k, 'count' : v })
            return items
        record = dict(
            seg_id=seg_state.id,
            ssvid=seg_state.ssvid,
            closed=seg_state.closed,
            noise=seg_state.noise,
            message_count=seg_state.msg_count,
            first_msg_timestamp=first_msg['timestamp'],
            first_msg_lat=first_msg['lat'],
            first_msg_lon=first_msg['lon'],
            first_msg_course=first_msg['course'],
            first_msg_speed=first_msg['speed'],
            last_msg_timestamp=last_msg['timestamp'],
            last_msg_lat=last_msg['lat'],
            last_msg_lon=last_msg['lon'],
            last_msg_course=last_msg['course'],
            last_msg_speed=last_msg['speed'],
            timestamp=timestamp,
            shipnames=sig2rcd('shipnames'),
            callsigns=sig2rcd('callsigns'),
            imos=sig2rcd('imos'),
            transponders=sig2rcd('transponders')
        )
        for field, stats in self.stats_fields:
            stat_values = ms.field_stats(field)
            for stat in stats:
                record[self.stat_output_field_name(field, stat)] = stat_values.get(stat, None)
        if has_timestamp:
            if len(messages):
                record['timestamp_min'] = messages[0]['timestamp']
                record['timestamp_max'] = messages[-1]['timestamp']
                record['timestamp_first'] = messages[0]['timestamp']
                record['timestamp_last'] = messages[-1]['timestamp']
            else:
                record['timestamp_min'] = record['timestamp_max'] = None
                record['timestamp_first'] = record['timestamp_last'] = None
            record['timestamp_count'] = len(messages)
        return record

    def _segment_state(self, seg_record):
        first_msg = {
                'ssvid': seg_record['ssvid'],
                'timestamp': seg_record['first_msg_timestamp'],
                'lat': seg_record['first_msg_lat'],
                'lon': seg_record['first_msg_lon'],
                'course' : seg_record['first_msg_course'],
                'speed' : seg_record['first_msg_speed']
            }
        last_msg = {
                'ssvid': seg_record['ssvid'],
                'timestamp': seg_record['last_msg_timestamp'],
                'lat': seg_record['last_msg_lat'],
                'lon': seg_record['last_msg_lon'],
                'course' : seg_record['last_msg_course'],
                'speed' : seg_record['last_msg_speed']
            }
        def rcd2sig(name):
            return {d['value'] : d['count'] for d in seg_record[name]}
        signature = {
            'shipnames' : rcd2sig('shipnames'),
            'callsigns' : rcd2sig('callsigns'),
            'imos' : rcd2sig('imos'),
            'transponders' : rcd2sig('transponders'),
        }
        return SegmentState(id = seg_record['seg_id'],
                            noise = False,
                            closed = seg_record['closed'],
                            ssvid = seg_record['ssvid'],
                            msg_count = seg_record['message_count'],
                            first_msg = first_msg,
                            last_msg = last_msg,
                            opaque = {'signature' : signature}) 


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
        for k1 in ['shipnames', 'callsigns', 'imos', 'n_shipnames', 'n_callsigns', 'n_imos']:
            msg.pop(k1, None)
        return msg

    def _extract_startup_info(self, msgs):
        msgids = {}
        locations = {}
        info = {}
        if msgs:
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
                Segmentizer.store_info(info, msg)
            msgs = msgs[i:]
        return msgs, msgids, locations, info

    def _pruned_info(self, info, start_of_current_day):
        """Keep only info within INFO_PING_INTERVAL_MINS of the start of the next day.

        Only keep data from before the next day so we don't count info twice.
        """
        new_info = {}
        start_of_next_day = start_of_current_day + dt.timedelta(days=1)
        t0 = start_of_next_day - dt.timedelta(minutes=INFO_PING_INTERVAL_MINS)
        t1 = start_of_next_day
        for ts in info:
            if t0 <= ts <= t1:
                new_info[ts] = info[ts]
        return new_info

    @staticmethod
    def _update_sig_part(sig, msgs, inner_key, outer_key):
        counter = Counter(sig.get(outer_key, {}))
        for m in msgs:
            for k, cnt in m[outer_key].items():
                counter.update({k : cnt})
        sig[outer_key] = dict(counter)


    def _get_signature(self, seg):
        # 1. get current signatures from seg.opaque
        sig = seg.opaque.get('signature', {}).copy()
        a_types = set(['AIS.1', 'AIS.2', 'AIS.3'])
        b_types = set(['AIS.18', 'AIS.19'])
        counts = sig.get('transponders', {'is_A' : 0, 'is_B' : 0})
        # TODO: could easily decay (exponential moving average) the signature
        # Parts here but mulitplying earlier days by 0.9 or some such.
        a_cnt = counts.get('is_A', 0)
        b_cnt = counts.get('is_B', 0)
        for msg in seg.msgs:
            a_cnt += msg['type'] in a_types
            b_cnt += msg['type'] in b_types
        sig['transponders'] = {'is_A' : a_cnt, 'is_B' : b_cnt}
        self._update_sig_part(sig, seg.msgs, 'shipname', 'shipnames')
        self._update_sig_part(sig, seg.msgs, 'callsign', 'callsigns')
        self._update_sig_part(sig, seg.msgs, 'imo', 'imos')
        return sig

    def _as_record(self, record):
        """Return everything except noise and the stats fields"""
        record = record.copy()
        record.pop('noise')
        for field, stats in self.stats_fields:
            for stat in stats:
                record.pop(self.stat_output_field_name(field, stat))
        return record

    def _as_record_v1(self, record):
        """Munge to be compatible with old schema"""
        record = record.copy()
        record.pop('closed')
        record['origin_ts'] = record.pop('first_msg_timestamp')
        for k in ['lat', 'lon', 'course', 'speed']:
            record.pop('first_msg_' + k)
        record['last_pos_ts'] = record.pop('last_msg_timestamp')
        record['last_pos_lat'] = record.pop('last_msg_lat')
        record['last_pos_lon'] = record.pop('last_msg_lon')
        record.pop('last_msg_course')
        record.pop('last_msg_speed')
        record.pop('shipnames')
        record.pop('callsigns')
        record.pop('imos')
        record.pop('transponders')
        return record

    def segment(self, messages, seg_records):
        messages, msgids, locations, info = self._extract_startup_info(messages)
        for timestamp, grouped_msgs in self._windowed_groups(messages, self.look_ahead):
            date = timestamp.date()
            seg_states = [self._segment_state(rcd) for rcd in seg_records]
            segments = Segmentizer.from_seg_states(seg_states, grouped_msgs, 
                        prev_msgids=msgids,
                        prev_locations=locations,
                        prev_info=info, 
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
                        signature = self._get_signature(seg)
                        rcd = self._segment_record(seg.state, seg.msgs, timestamp, signature)
                        rcd['closed'] = is_closed
                        output_rcd = rcd.copy()
                        output_rcd['timestamp'] = timestamp
                        yield (self.OUTPUT_TAG_SEGMENTS_V1, self._as_record_v1(output_rcd))
                        # Only store new style records, so that we get the ~same code path
                        # running over multiple days as running over a single day.
                        output_rcd = self._as_record(output_rcd)
                        yield (self.OUTPUT_TAG_SEGMENTS, output_rcd)
                        if not is_closed:
                            seg_records.append(output_rcd)
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
            info = self._pruned_info(segments.cur_info, timestamp)

