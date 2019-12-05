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

from pipe_tools.timestamp import datetimeFromTimestamp
from pipe_tools.timestamp import timestampFromDatetime

from segment_implementation import SegmentImplementation

logger = logging.getLogger(__file__)
logger.setLevel(logging.DEBUG)

class Segment(PTransform):

    def __init__(self,
                 start_date=None,
                 end_date=None,
                 segmenter_params = None,
                 look_ahead = 0,
                 stats_fields=None,
                 **kwargs):
        super(Segment, self).__init__(**kwargs)
        self._segmenter = SegmentImplementation(start_date, end_date, look_ahead, 
                                                stats_fields, segmenter_params)

    @property
    def OUTPUT_TAG_SEGMENTS(self):
        return self._segmenter.OUTPUT_TAG_SEGMENTS

    @property
    def OUTPUT_TAG_SEGMENTS_V1(self):
        return self._segmenter.OUTPUT_TAG_SEGMENTS_V1

    @property
    def OUTPUT_TAG_MESSAGES(self):
        return self._segmenter.OUTPUT_TAG_MESSAGES
    

    @staticmethod
    def _convert_message_in(msg):
        msg = dict(msg)
        msg['raw_timestamp'] = msg['timestamp']
        msg['timestamp'] = datetimeFromTimestamp(msg['raw_timestamp'])
        return msg

    @staticmethod
    def _convert_message_out(msg):
        msg = dict(msg)
        msg['timestamp'] = msg.pop('raw_timestamp')
        return msg

    @staticmethod
    def _convert_segment_in(seg):
        seg = dict(seg.items())
        for k in ['timestamp', 'first_msg_timestamp', 'last_msg_timestamp']:
            seg[k] = datetimeFromTimestamp(seg[k])
        return seg

    @staticmethod
    def _convert_segment_out(seg):
        seg = dict(seg.items())
        for k in ['timestamp', 'first_msg_timestamp', 'last_msg_timestamp',
                  'timestamp_first', 'timestamp_last', # Stats stuff TODO: clean out someday
                  'timestamp_min', 'timestamp_max']:
            if k in seg and not seg[k] is None:
                seg[k] = timestampFromDatetime(seg[k])

        return seg



    def segment(self, kv):
        key, seg_mes_map = kv
        segments = [self._convert_segment_in(x) for x in seg_mes_map['segments']]
        messages = sorted(seg_mes_map['messages'], key=lambda msg: msg['timestamp'])
        messages = [self._convert_message_in(x) for x in messages]
        logger.debug('Segmenting key %r sorted %s messages and %s segments',
                        key, len(messages), len(segments))
        for key, value in self._segmenter.segment(messages, segments):
            if key == self.OUTPUT_TAG_MESSAGES:
                msg = self._convert_message_out(value)
                yield msg
            elif key in (self.OUTPUT_TAG_SEGMENTS_V1, self.OUTPUT_TAG_SEGMENTS): 
                yield TaggedOutput(key, self._convert_segment_out(value))
            else:
                logger.warning('Unknown key in segment.segment (%)', key)

    def expand(self, xs):
        return (
            xs | FlatMap(self.segment)
                .with_outputs(self.OUTPUT_TAG_SEGMENTS_V1, 
                              self.OUTPUT_TAG_SEGMENTS,
                              main=self.OUTPUT_TAG_MESSAGES)
        )

    @property
    def segment_schema(self):
        schema = bigquery.TableSchema()

        def add_field(name, field_type, mode='REQUIRED'):
            field = bigquery.TableFieldSchema()
            field.name = name
            field.type = field_type
            field.mode = mode
            schema.fields.append(field)

        add_field('seg_id', 'STRING')
        add_field('ssvid',  'STRING')
        add_field('closed', 'BOOLEAN')
        add_field('message_count', 'INTEGER')
        add_field('timestamp', 'TIMESTAMP')
        for prefix in ['first_msg_', 'last_msg_', 'first_msg_on_day_', 'last_msg_on_day_']:
            add_field(prefix + 'timestamp', 'TIMESTAMP')
            add_field(prefix + 'lat', 'FLOAT')
            add_field(prefix + 'lon', 'FLOAT')
            add_field(prefix + 'course', 'FLOAT')
            add_field(prefix + 'speed', 'FLOAT')

        def add_sig_field(name):
            field = bigquery.TableFieldSchema()
            field.name = name
            field.type = "RECORD"
            field.mode = "REPEATED"
            f1 = bigquery.TableFieldSchema()
            f1.name = 'value'
            f1.type = 'STRING'
            f2 =  bigquery.TableFieldSchema()
            f2.name = 'count'
            f2.type = 'INTEGER'
            field.fields = [f1, f2]
            schema.fields.append(field)

        add_sig_field('shipnames')
        add_sig_field('callsigns')
        add_sig_field('imos')
        add_sig_field('transponders')

        return schema


    @property
    def segment_schema_v1(self):
        DEFAULT_FIELD_TYPE = 'STRING'
        FIELD_TYPES = {
            'timestamp': 'TIMESTAMP',
            'lat': 'FLOAT',
            'lon': 'FLOAT',
            'imo': 'INTEGER',
        }

        schema = bigquery.TableSchema()

        def add_field(name, field_type, mode='REQUIRED'):
            field = bigquery.TableFieldSchema()
            field.name = name
            field.type = field_type
            field.mode = mode
            schema.fields.append(field)

        add_field('seg_id', 'STRING')
        add_field('ssvid',  'STRING')
        add_field('noise', 'BOOLEAN')
        add_field('message_count', 'INTEGER')
        add_field('timestamp', 'TIMESTAMP')
        add_field('origin_ts', 'TIMESTAMP')
        add_field('last_pos_ts', 'TIMESTAMP')
        add_field('last_pos_lat', 'FLOAT')
        add_field('last_pos_lon', 'FLOAT')

        STAT_TYPES = {
            'most_common_count': 'INTEGER',
            'count': 'INTEGER'
        }

        for field_name, stats in self._segmenter.stats_fields:
            for stat_name in stats:
                add_field(self._segmenter.stat_output_field_name(field_name, stat_name),
                          STAT_TYPES.get(stat_name) or FIELD_TYPES.get(field_name, DEFAULT_FIELD_TYPE),
                          mode='NULLABLE')


        return schema


