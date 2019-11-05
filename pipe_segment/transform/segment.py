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

from segment_implementation import SegmentImplementation

logger = logging.getLogger(__file__)
logger.setLevel(logging.DEBUG)

class Segment(PTransform):

    OUTPUT_TAG_MESSAGES = 'messages'
    OUTPUT_TAG_SEGMENTS = 'segments'

    def __init__(self,
                 start_date=None,
                 end_date=None,
                 segmenter_params = None,
                 look_ahead = 0,
                 **kwargs):
        super(Segment, self).__init__(**kwargs)
        self._segmenter = SegmentImplementation(start_date, end_date, look_ahead, segmenter_params)

    @staticmethod
    def _convert_message_in(msg):
        msg = dict(msg)
        msg['raw_timestamp'] = msg['timestamp']
        msg['timestamp'] = datetimeFromTimestamp(msg['raw_timestamp'])
        return msg

    @staticmethod
    def _convert_message_out(msg):
        msg = JSONDict(msg)
        msg['timestamp'] = msg.pop('raw_timestamp')
        # Ensure all messages have collected info fields
        for k in ['shipnames', 'callsigns', 'imos']:
            if k not in msg:
                msg[k] = []
        return msg

    @staticmethod
    def _convert_segment_in(seg):
        seg = dict(seg.items())
        for k in ['timestamp', 'first_timestamp', 'last_timestamp']:
            seg[k] = datetimeFromTimestamp(seg[k])
        return seg

    @staticmethod
    def _convert_segment_out(seg):
        seg = JSONDict(seg.items())
        for k in ['timestamp', 'first_timestamp', 'last_timestamp']:
            seg[k] = timestampFromDatetime(seg[k])
        return seg

    def segment(self, kv):
        key, seg_mes_map = kv
        segments = [self._convert_segment_in(x) for x in seg_mes_map['segments']]
        messages = sorted(seg_mes_map['messages'], key=lambda msg: msg['timestamp'])
        messages = [self._convert_message_in(x) for x in messages]
        logger.debug('Segmenting key %r sorted %s messages and %s segments',
                        key, len(messages), len(segments))
        first = True
        for key, value in self._segmenter.segment(messages, segments):
            if key == self._segmenter.OUTPUT_TAG_MESSAGES:
                msg = self._convert_message_out(value)
                if first:
                    logging.info('emitted_msg_example', msg)
                    first = False
                yield msg
            elif key == self._segmenter.OUTPUT_TAG_SEGMENTS:
                yield TaggedOutput(self._segmenter.OUTPUT_TAG_SEGMENTS,
                                self._convert_segment_out(value))
            else:
                logger.warning('Unknown key in segment.segment (%)', key)

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

        field = bigquery.TableFieldSchema()
        field.name = "shipnames"
        field.type = "RECORD"
        field.mode = "REPEATED"
        f1 = bigquery.TableFieldSchema()
        f1.name = 'shipname'
        f1.type = 'STRING'
        f1.mode = "REQUIRED"
        f2 = bigquery.TableFieldSchema()
        f2.name = 'cnt'
        f2.type = 'INTEGER'
        f2.mode = "REQUIRED"
        field.fields = [f1, f2]
        schema.fields.append(field)

        field = bigquery.TableFieldSchema()
        field.name = "callsigns"
        field.type = "RECORD"
        field.mode = "REPEATED"
        f1 = bigquery.TableFieldSchema()
        f1.name = 'callsign'
        f1.type = 'STRING'
        f2 = bigquery.TableFieldSchema()
        f2.name = 'cnt'
        f2.type = 'INTEGER'
        field.fields = [f1, f2]
        schema.fields.append(field)

        field = bigquery.TableFieldSchema()
        field.name = "imos"
        field.type = "RECORD"
        field.mode = "REPEATED"
        f1 = bigquery.TableFieldSchema()
        f1.name = 'imo'
        f1.type = 'STRING'
        f2 = bigquery.TableFieldSchema()
        f2.name = 'cnt'
        f2.type = 'INTEGER'
        field.fields = [f1, f2]
        schema.fields.append(field)
        return schema
