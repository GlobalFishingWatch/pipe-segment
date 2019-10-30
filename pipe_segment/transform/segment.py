import bisect
import datetime as dt
import itertools as it
import logging
import six
import pytz

import apache_beam as beam
from apache_beam import PTransform
from apache_beam import FlatMap
from apache_beam.pvalue import AsDict
from apache_beam.pvalue import TaggedOutput
from apache_beam.io.gcp.internal.clients import bigquery

from gpsdio_segment.core import Segmentizer
from gpsdio_segment.segment import SegmentState

from pipe_tools.coders import JSONDict
from pipe_tools.timestamp import datetimeFromTimestamp
from pipe_tools.timestamp import timestampFromDatetime

logger = logging.getLogger(__file__)
logger.setLevel(logging.DEBUG)

inf = float("inf")

class Segment(PTransform):

    OUTPUT_TAG_MESSAGES = 'messages'
    OUTPUT_TAG_SEGMENTS = 'segments'

    def __init__(self,
                 segmenter_params = None,
                 lookahead = 0,
                 **kwargs):
        super(Segment, self).__init__(**kwargs)
        self.lookahead = lookahead
        self.segmenter_params = segmenter_params or {}

    @staticmethod
    def _convert_messages_in(msg):
        msg = dict(msg)
        msg['raw_timestamp'] = msg['timestamp']
        msg['timestamp'] = datetimeFromTimestamp(msg['raw_timestamp'])
        return msg

    # TODO: remove
    @staticmethod
    def _key_by_day(msg):
        """Return the timestamp corresponding to the beginning of day"""
        return timestampFromDatetime(
            dt.datetime.combine(msg['timestamp'].date(), dt.time()).replace(tzinfo=pytz.utc))

    @staticmethod
    def _convert_messages_out(msg, seg_id):
        msg = JSONDict(msg)
        msg['seg_id'] = seg_id
        msg['timestamp'] = msg.pop('raw_timestamp')
        return msg


    def _segment_record(self, seg_state, timestamp):
        first_msg = seg_state.first_msg
        last_msg = seg_state.last_msg
        return JSONDict (
            seg_id=seg_state.id,
            ssvid=seg_state.ssvid,
            closed=seg_state.closed,
            message_count=seg_state.msg_count,
            first_timestamp=timestampFromDatetime(first_msg['timestamp']),
            first_lat=first_msg['lat'],
            first_lon=first_msg['lon'],
            first_course=first_msg['course'],
            first_speed=first_msg['speed'],
            last_timestamp=timestampFromDatetime(last_msg['timestamp']),
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
                            first_msg = self._convert_messages_in(first_msg),
                            last_msg = self._convert_messages_in(last_msg))

    def _gpsdio_segment(self, messages, seg_records):
        messages = it.imap(self._convert_messages_in, messages)
        for timestamp, messages in it.groupby(messages, self._key_by_day):
            seg_states = [self._segment_state(rcd) for rcd in seg_records]
            segments = Segmentizer.from_seg_states(seg_states, messages, **self.segmenter_params)
            seg_records = []
            for seg in segments:
                if not seg.noise:
                    logger.debug('Segmenting key %r yielding segment %s containing %s messages ' % (seg.ssvid, seg.id, len(seg)))
                    rcd = self._segment_record(seg.state, timestamp)
                    yield TaggedOutput(Segment.OUTPUT_TAG_SEGMENTS, rcd)
                    if not seg.closed:
                        seg_records.append(rcd)
                #     is_closed = seg.closed
                #     if seg.last_message['timestamp'].date() > date:
                #         is_closed = False
                #     seg.msgs = [x for x in seg.msgs if x['timestamp'].date() <= date]
                #     if seg.msgs or seg.prev_state:
                #         logger.debug('Segmenting key %r yielding segment %s containing %s messages ' % (seg.ssvid, seg.id, len(seg)))
                #         rcd = self._segment_record(seg.state, timestamp, )
                #         rcd['closed'] = is_closed
                #         yield TaggedOutput(Segment.OUTPUT_TAG_SEGMENTS, rcd)
                #         if not is_closed:
                #             seg_records.append(rcd)
                seg_id = None if seg.noise else seg.id
                for msg in seg:
                    msg = self._convert_messages_out(msg, seg_id)
                    yield msg

    @staticmethod
    def _date_of(msg):
        return dt.datetime.combine(msg['timestamp'].date(), dt.time()).replace(tzinfo=pytz.utc)

    def _windowed_groups(self, messages, n_days):
        """yield overlapping sequences of n_days shifted by one day each time"""
        if not isinstance(messages, list):
            messages = list(messages)
        dates = [self._date_of(msg) for msg in messages]
        first_date = dates[0]
        while True:
            last_date = first_date + dt.timedelta(days=n_days-1)
            i0 = bisect.bisect_left(dates, first_date)
            i1 = bisect.bisect_right(dates, last_date)
            if i0 == i1:
                raise StopIteration
            yield first_date, messages[i0:i1]
            first_date += dt.timedelta(days=1)

    def segment(self, kv):
        key, seg_mes_map = kv
        segments = list(seg_mes_map['segments'])
        messages = sorted(seg_mes_map['messages'], key=lambda msg: msg['timestamp'])
        logger.debug('Segmenting key %r sorted %s messages and %s segments',
                        key, len(messages), len(segments))
        for item in self._gpsdio_segment(messages, segments):
            yield item

    def expand(self, xs):
        return (
            xs | FlatMap(self.segment)
                .with_outputs(self.OUTPUT_TAG_SEGMENTS, main=self.OUTPUT_TAG_MESSAGES)

        )


    @property
    def segment_schema(self):
        schema = bigquery.TableSchema()

        field = bigquery.TableFieldSchema()
        field.name = "seg_id"
        field.type = "STRING"
        field.mode = "REQUIRED"
        schema.fields.append(field)

        field = bigquery.TableFieldSchema()
        field.name = "ssvid"
        field.type = "STRING"
        field.mode = "REQUIRED"
        schema.fields.append(field)

        field = bigquery.TableFieldSchema()
        field.name = "closed"
        field.type = "BOOLEAN"
        field.mode = "REQUIRED"
        schema.fields.append(field)

        field = bigquery.TableFieldSchema()
        field.name = "message_count"
        field.type = "INTEGER"
        field.mode = "REQUIRED"
        schema.fields.append(field)

        field = bigquery.TableFieldSchema()
        field.name = "timestamp"
        field.type = "TIMESTAMP"
        field.mode = "REQUIRED"
        schema.fields.append(field)

        field = bigquery.TableFieldSchema()
        field.name = "first_timestamp"
        field.type = "TIMESTAMP"
        field.mode = "REQUIRED"
        schema.fields.append(field)

        field = bigquery.TableFieldSchema()
        field.name = "first_lat"
        field.type = "FLOAT"
        schema.fields.append(field)

        field = bigquery.TableFieldSchema()
        field.name = "first_lon"
        field.type = "FLOAT"
        schema.fields.append(field)

        field = bigquery.TableFieldSchema()
        field.name = "first_course"
        field.type = "FLOAT"
        schema.fields.append(field)

        field = bigquery.TableFieldSchema()
        field.name = "first_speed"
        field.type = "FLOAT"
        schema.fields.append(field)

        field = bigquery.TableFieldSchema()
        field.name = "last_timestamp"
        field.type = "TIMESTAMP"
        field.mode = "REQUIRED"
        schema.fields.append(field)

        field = bigquery.TableFieldSchema()
        field.name = "last_lat"
        field.type = "FLOAT"
        schema.fields.append(field)

        field = bigquery.TableFieldSchema()
        field.name = "last_lon"
        field.type = "FLOAT"
        schema.fields.append(field)

        field = bigquery.TableFieldSchema()
        field.name = "last_course"
        field.type = "FLOAT"
        schema.fields.append(field)

        field = bigquery.TableFieldSchema()
        field.name = "last_speed"
        field.type = "FLOAT"
        schema.fields.append(field)

        return schema
