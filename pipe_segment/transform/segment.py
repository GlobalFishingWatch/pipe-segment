import datetime as dt
import itertools as it
import logging
import six

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
logger.setLevel(logging.INFO)

class Segment(PTransform):

    OUTPUT_TAG_MESSAGES = 'messages'
    OUTPUT_TAG_SEGMENTS = 'segments'

    def __init__(self,
                 segmenter_params = None,
                 **kwargs):
        super(Segment, self).__init__(**kwargs)
        self.segmenter_params = segmenter_params or {}

    @staticmethod
    def _convert_messages_in(msg):
        msg = dict(msg)
        msg['raw_timestamp'] = msg['timestamp']
        msg['timestamp'] = datetimeFromTimestamp(msg['raw_timestamp'])
        return msg

    @staticmethod
    def _key_by_day(msg):
        return msg['timestamp'].toordinal()

    @staticmethod
    def _convert_messages_out(msg, seg_id):
        msg = JSONDict(msg)
        msg['seg_id'] = seg_id
        msg['timestamp'] = msg.pop('raw_timestamp')
        return msg


    def _segment_record(self, seg_state):
        last_msg = seg_state.last_msg
        return JSONDict (
            seg_id=seg_state.id,
            # TODO: ssvid everywhere
            ssvid=seg_state.ssvid,
            closed=seg_state.closed,
            message_count=seg_state.msg_count,
            last_timestamp=timestampFromDatetime(last_msg['timestamp']),
            last_lat=last_msg['lat'],
            last_lon=last_msg['lon'],
            last_course=last_msg['course'],
            last_speed=last_msg['speed'],
        )

    def _segment_state(self, seg_record):
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
                            last_msg = self._convert_messages_in(last_msg))

    def _gpsdio_segment(self, messages, seg_records):
        messages = it.imap(self._convert_messages_in, messages)
        seg_states = it.imap(self._segment_state, seg_records)
        for key, messages in it.groupby(messages, self._key_by_day):
            segments = Segmentizer.from_seg_states(seg_states, messages, **self.segmenter_params)
            seg_states = []
            for seg in segments:
                if not seg.closed:
                    seg_states.append(seg.state)
                seg_id = None if seg.noise else seg.id
                for msg in seg:
                    yield self._convert_messages_out(msg, seg_id)
                if not seg.noise:
                    logger.debug('Segmenting key %r yielding segment %s containing %s messages ' % (seg.ssvid, seg.id, len(seg)))
                    yield TaggedOutput(Segment.OUTPUT_TAG_SEGMENTS, self._segment_record(seg.state))


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
