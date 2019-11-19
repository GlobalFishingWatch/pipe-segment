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
    def OUTPUT_TAG_OLD_SEGMENTS(self):
        return self._segmenter.OUTPUT_TAG_OLD_SEGMENTS

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
    # TODO: do this for timestamp stats fields
    def _convert_segment_in(seg):
        seg = dict(seg.items())
        for k in ['timestamp', 'first_msg_timestamp', 'last_msg_timestamp']:
            seg[k] = datetimeFromTimestamp(seg[k])
        # Stats
        for k in ['timestamp_first', 'timest_last', 'timestamp_min', 'timestamp_max',
                  'timestamp_count']:
            if k in seg:
                seg[k] = datetimeFromTimestamp(seg[k])
        return seg

    @staticmethod
    def _convert_segment_out(seg):
        # TODO: do this for timestamp stats fields
        seg = dict(seg.items())
        for k in ['timestamp', 'first_msg_timestamp', 'last_msg_timestamp']:
            seg[k] = timestampFromDatetime(seg[k])
        # Stats
        for k in ['timestamp_first', 'timest_last', 'timestamp_min', 'timestamp_max',
                  'timestamp_count']:
            if k in seg:
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
            if key == self._segmenter.OUTPUT_TAG_MESSAGES:
                msg = self._convert_message_out(value)
                yield msg
            elif key == self._segmenter.OUTPUT_TAG_SEGMENTS:
                yield TaggedOutput(self._segmenter.OUTPUT_TAG_SEGMENTS,
                                self._convert_segment_out(value))
            else:
                logger.warning('Unknown key in segment.segment (%)', key)

    def expand(self, xs):
        return (
            xs | FlatMap(self.segment)
                .with_outputs(self._segmenter.OUTPUT_TAG_SEGMENTS, main=self._segmenter.OUTPUT_TAG_MESSAGES)
        )

    @property
    def segment_schema_v2(self):
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
        for prefix in ['first_msg_', 'last_msg_']:
            add_field(prefix + 'timestamp', 'TIMESTAMP')
            add_field(prefix + 'lat', 'FLOAT')
            add_field(prefix + 'lon', 'FLOAT')
            add_field(prefix + 'course', 'FLOAT')
            add_field(prefix + 'speed', 'FLOAT')

        # TODO: add signature fields

        return schema


    @property
    def segment_schema_v1(self):
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
        add_field('origin_ts', )
        add_field('last_timestamp', 'TIMESTAMP')
        add_field('last_lat', 'FLOAT')
        add_field('last_lon', 'FLOAT')

        STAT_TYPES = {
            'most_common_count': 'INTEGER',
            'count': 'INTEGER'
        }

        for field_name, stats in self.stats_fields:
            for stat_name in stats:
                add_field(Segment.stat_output_field_name(field_name, stat_name),
                          STAT_TYPES.get(stat_name) or FIELD_TYPES.get(field_name, DEFAULT_FIELD_TYPE),
                          mode='NULLABLE')


        return schema




    # DEFAULT_STATS_FIELDS = [('lat', MessageStats.NUMERIC_STATS),
    #                         ('lon', MessageStats.NUMERIC_STATS),
    #                         ('timestamp', MessageStats.NUMERIC_STATS),
    #                         ('shipname', MessageStats.FREQUENCY_STATS),
    #                         ('imo', MessageStats.FREQUENCY_STATS),
    #                         ('callsign', MessageStats.FREQUENCY_STATS)]






# For messages, fill in shipname, callsign, imo from
# their most common values, then compute n_shipname, n_callsign, n_imo
# as before. Compute necessary stats for segments, add to segments, then
# remove stats from messages

#(do this in segment_implementation.)



# For new segment, pop noise
# for old segment:
# origin_ts <- first_timestamp
# pop first_lat, first_lon, first_speed, first_course, last_speed, last_course
# pop closed
# 

    #     field.name = "seg_id"
    #     field.name = "message_count"
    #     field.name = "ssvid"
    #     field.name = "noise"
    #     field.name = "timestamp"
    #     field.name = "origin_ts"
    #     field.name = "last_pos_ts"
    #     field.name = "last_pos_lat"
    #     field.name = "last_pos_lon"

    # DEFAULT_STATS_FIELDS = [('lat', MessageStats.NUMERIC_STATS),
    #                         ('lon', MessageStats.NUMERIC_STATS),
    #                         ('timestamp', MessageStats.NUMERIC_STATS),
    #                         ('shipname', MessageStats.FREQUENCY_STATS),
    #                         ('imo', MessageStats.FREQUENCY_STATS),
    #                         ('callsign', MessageStats.FREQUENCY_STATS)]

    #     for field_name, stats in self.stats_fields:
    #         for stat_name in stats:
    #             field = bigquery.TableFieldSchema()
    #             field.name = Segment.stat_output_field_name(field_name, stat_name)
    #             field.type = STAT_TYPES.get(stat_name) or FIELD_TYPES.get(field_name, DEFAULT_FIELD_TYPE)
    #             field.mode = "NULLABLE"
    #             schema.fields.append(field)

    #     STAT_TYPES = {
    #         'most_common_count': 'INTEGER',
    #         'count': 'INTEGER'
    #     }

