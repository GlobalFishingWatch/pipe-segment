import datetime as dt
import itertools as it

from apache_beam import PTransform
from apache_beam import FlatMap
from apache_beam import typehints
from apache_beam.pvalue import TaggedOutput
from apache_beam.io.gcp.internal.clients import bigquery

from gpsdio_segment.core import Segmentizer
from gpsdio_segment.core import BadSegment
from gpsdio_segment.core import NoiseSegment
from gpsdio_segment.core import SegmentState

from pipe_tools.coders import JSONDict
from pipe_tools.timestamp import datetimeFromTimestamp
from pipe_tools.timestamp import timestampFromDatetime

from pipe_segment.stats import MessageStats


class Segment(PTransform):

    OUTPUT_TAG_MESSAGES = 'messages'
    OUTPUT_TAG_SEGMENTS = 'segments'

    DEFAULT_STATS_FIELDS = [('lat', MessageStats.NUMERIC_STATS),
                            ('lon', MessageStats.NUMERIC_STATS),
                            ('timestamp', MessageStats.NUMERIC_STATS),
                            ('shipname', MessageStats.FREQUENCY_STATS),
                            ('imo', MessageStats.FREQUENCY_STATS),
                            ('callsign', MessageStats.FREQUENCY_STATS)]

    def __init__(self, segmenter_params = None,
                 stats_fields=DEFAULT_STATS_FIELDS,
                 **kwargs):
        super(Segment, self).__init__(**kwargs)
        self.segmenter_params = segmenter_params or {}
        self.stats_fields = stats_fields

    @staticmethod
    def _convert_messages_in(msg):
        msg = dict(msg)
        msg['timestamp'] = datetimeFromTimestamp(msg['timestamp'])
        msg['mmsi'] = msg['ssvid']
        return msg

    @staticmethod
    def _key_by_day(msg):
        return msg['timestamp'].toordinal()

    @staticmethod
    def _convert_messages_out(msg, seg_id):
        msg = JSONDict(msg)
        timestamp = timestampFromDatetime(msg['timestamp'])
        msg['timestamp'] = timestamp
        msg['seg_id'] = seg_id
        del msg['mmsi']
        return msg

    @staticmethod
    def stat_output_field_name (field_name, stat_name):
        return '%s_%s' % (field_name, stat_name)

    @classmethod
    def stat_output_field_names (cls, stat_fields):
        for field, stats in stat_fields:
            for stat in stats:
                yield cls.stat_output_field_name(field, stat)

    def _segment_record(self, messages, seg_state, seg_id):

        stats_numeric_fields = [f for f, stats in self.stats_fields if set(stats) & set (MessageStats.NUMERIC_STATS)]
        stats_frequency_fields = [f for f, stats in self.stats_fields if set(stats) & set (MessageStats.FREQUENCY_STATS)]

        ms = MessageStats(messages, stats_numeric_fields, stats_frequency_fields)

        first_msg = seg_state.msgs[0]
        last_pos_msg = first_msg
        for msg in seg_state.msgs:
            if msg.get('lat') is not None:
                last_pos_msg = msg

        record = JSONDict (
            seg_id=seg_id,
            ssvid=seg_state.mmsi,
            message_count=seg_state.msg_count,
            origin_ts=timestampFromDatetime(first_msg['timestamp']),
            timestamp=messages[-1]['timestamp'],
            last_pos_ts=timestampFromDatetime(last_pos_msg['timestamp']),
            last_pos_lat=last_pos_msg.get('lat'),
            last_pos_lon=last_pos_msg.get('lon')
        )

        for field, stats in self.stats_fields:
            stat_values = ms.field_stats(field)
            for stat in stats:
                record[self.stat_output_field_name(field, stat)] = stat_values.get(stat, None)

        return record

    def _segment_state (self, seg_record):
        messages = []
        if seg_record['origin_ts'] != seg_record['timestamp']:
            messages.append({
                'ssvid': seg_record['ssvid'],
                'timestamp': seg_record['timestamp'],
            })
        messages.append({
            'ssvid': seg_record['ssvid'],
            'timestamp': seg_record['timestamp'],
            'lat': seg_record['last_pos_lat'],
            'lon': seg_record['last_pos_lon']
        })

        state = SegmentState()
        state.id=seg_record['seg_id']
        state.mmsi=seg_record['ssvid']
        state.msg_count=seg_record['message_count']
        state.msgs = it.imap(self._convert_messages_in, messages)

        return state

    def _gpsdio_segment(self, messages, seg_records):

        messages = it.imap(self._convert_messages_in, messages)
        seg_states = it.imap(self._segment_state, seg_records)
        for key, messages in it.groupby(messages, self._key_by_day):
            segments = list(Segmentizer.from_seg_states(seg_states, messages, **self.segmenter_params))
            seg_states = []
            for seg in segments:
                seg_state = seg.state

                if isinstance(seg, BadSegment):
                    seg_id = "{}-BAD".format(seg.id)
                elif isinstance(seg, NoiseSegment):
                    seg_id = "{}-NOISE".format(seg.id)
                else:
                    seg_id = seg.id
                    seg_states.append(seg_state)

                seg_messages = list(it.imap(self._convert_messages_out, seg, it.repeat(seg_id)))

                seg_record = self._segment_record(seg_messages, seg_state, seg_id)
                yield TaggedOutput(Segment.OUTPUT_TAG_SEGMENTS, seg_record)

                for msg in seg_messages:
                    yield msg

    def segment(self, kv):
        key, values = kv
        messages = values['messages']
        segments = values['segments']

        messages = sorted(messages, key=lambda msg: msg['timestamp'])
        for item in self._gpsdio_segment(messages, segments):
            yield item

    def expand(self, xs):
        return (
            xs | FlatMap(self.segment)
                .with_outputs(self.OUTPUT_TAG_SEGMENTS, main=self.OUTPUT_TAG_MESSAGES)

        )

    @property
    def segment_schema(self):
        DEFAULT_FIELD_TYPE = 'STRING'
        FIELD_TYPES = {
            'timestamp': 'TIMESTAMP',
            'lat': 'FLOAT',
            'lon': 'FLOAT',
            'imo': 'INTEGER',
        }

        STAT_TYPES = {
            'most_common_count': 'INTEGER',
            'count': 'INTEGER'
        }

        schema = bigquery.TableSchema()

        field = bigquery.TableFieldSchema()
        field.name = "seg_id"
        field.type = "STRING"
        field.mode = "REQUIRED"
        schema.fields.append(field)

        field = bigquery.TableFieldSchema()
        field.name = "message_count"
        field.type = "INTEGER"
        field.mode = "REQUIRED"
        schema.fields.append(field)

        field = bigquery.TableFieldSchema()
        field.name = "ssvid"
        field.type = "INTEGER"
        field.mode = "REQUIRED"
        schema.fields.append(field)

        field = bigquery.TableFieldSchema()
        field.name = "timestamp"
        field.type = "TIMESTAMP"
        field.mode = "REQUIRED"
        schema.fields.append(field)

        field = bigquery.TableFieldSchema()
        field.name = "origin_ts"
        field.type = "TIMESTAMP"
        field.mode = "REQUIRED"
        schema.fields.append(field)

        field = bigquery.TableFieldSchema()
        field.name = "last_pos_ts"
        field.type = "TIMESTAMP"
        schema.fields.append(field)

        field = bigquery.TableFieldSchema()
        field.name = "last_pos_lat"
        field.type = "FLOAT"
        schema.fields.append(field)

        field = bigquery.TableFieldSchema()
        field.name = "last_pos_lon"
        field.type = "FLOAT"
        schema.fields.append(field)

        for field_name, stats in self.stats_fields:
            for stat_name in stats:
                field = bigquery.TableFieldSchema()
                field.name = Segment.stat_output_field_name(field_name, stat_name)
                field.type = STAT_TYPES.get(stat_name) or FIELD_TYPES.get(field_name, DEFAULT_FIELD_TYPE)
                field.mode = "NULLABLE"
                schema.fields.append(field)

        return schema
