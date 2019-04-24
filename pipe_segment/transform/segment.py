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

    def __init__(self, segments_pcoll_kv,
                 segmenter_params = None,
                 stats_fields=DEFAULT_STATS_FIELDS,
                 **kwargs):
        super(Segment, self).__init__(**kwargs)
        self.segmenter_params = segmenter_params or {}
        self.stats_fields = stats_fields
        self.segments_pcoll_kv=segments_pcoll_kv

    @staticmethod
    def _convert_messages_in(msg):
        def convert_type (t):
            if t is not None and t.startswith('AIS.'):
                try:
                    t = int(t[4:])
                except ValueError:
                    pass
            return t

        msg = dict(msg)
        update_fields = dict(
            mmsi = msg['ssvid'],
            timestamp = datetimeFromTimestamp(msg['timestamp']),
            type = convert_type(msg.get('type')),
            shipname = msg.get('n_shipname'),
            callsign = msg.get('n_callsign')
        )
        msg['__temp'] = {k:msg.get(k) for k in update_fields.keys()}
        msg.update(update_fields)

        return msg

    @staticmethod
    def _key_by_day(msg):
        return msg['timestamp'].toordinal()

    @staticmethod
    def _convert_messages_out(msg, seg_id):
        msg = JSONDict(msg)
        msg['seg_id'] = seg_id

        update_fields = msg['__temp']
        del msg['__temp']

        for k,v in six.iteritems(update_fields):
            if v is None:
                del msg[k]
            else:
                msg[k] = v

        return msg

    @staticmethod
    def stat_output_field_name (field_name, stat_name):
        return '%s_%s' % (field_name, stat_name)

    @classmethod
    def stat_output_field_names (cls, stat_fields):
        for field, stats in stat_fields:
            for stat in stats:
                yield cls.stat_output_field_name(field, stat)

    def _segment_record(self, messages, seg_state):

        stats_numeric_fields = [f for f, stats in self.stats_fields if set(stats) & set (MessageStats.NUMERIC_STATS)]
        stats_frequency_fields = [f for f, stats in self.stats_fields if set(stats) & set (MessageStats.FREQUENCY_STATS)]
        ms = MessageStats(messages, stats_numeric_fields, stats_frequency_fields)

        # Make sure the segmenter state messages are sorted by timestamp here,
        # as in some situations the last message in the state might not be the
        # latest and we want to use the actual last message here for timestamp
        # calculations
        sorted_msgs = sorted(seg_state.msgs, key=lambda x: x['timestamp'])
        first_msg = sorted_msgs[0]
        last_msg = sorted_msgs[-1]

        record = JSONDict (
            seg_id=seg_state.id,
            ssvid=str(seg_state.mmsi),
            noise=seg_state.noise,
            message_count=seg_state.msg_count,
            origin_ts=timestampFromDatetime(first_msg['timestamp']),
            timestamp=timestampFromDatetime(last_msg['timestamp']),
        )

        pos_messages = [msg for msg in sorted_msgs if msg.get('lat') is not None] or [None]
        last_pos_msg = pos_messages[-1]
        if last_pos_msg:
            record.update(dict(
                last_pos_ts=timestampFromDatetime(last_pos_msg['timestamp']),
                last_pos_lat=last_pos_msg['lat'],
                last_pos_lon=last_pos_msg['lon']
            ))

        for field, stats in self.stats_fields:
            stat_values = ms.field_stats(field)
            for stat in stats:
                record[self.stat_output_field_name(field, stat)] = stat_values.get(stat, None)

        return record

    def _segment_state (self, seg_record):
        messages = []
        if seg_record.get('last_pos_ts') is not None:
            messages.append({
                'ssvid': seg_record['ssvid'],
                'timestamp': seg_record['last_pos_ts'],
                'lat': seg_record['last_pos_lat'],
                'lon': seg_record['last_pos_lon']
            })
        if seg_record['origin_ts'] not in [msg['timestamp'] for msg in messages]:
            messages.insert(0, {
                'ssvid': seg_record['ssvid'],
                'timestamp': seg_record['origin_ts'],
            })
        if seg_record['timestamp'] not in [msg['timestamp'] for msg in messages]:
            messages.append({
                'ssvid': seg_record['ssvid'],
                'timestamp': seg_record['timestamp'],
            })

        state = SegmentState()
        state.id=seg_record['seg_id']
        state.noise=seg_record['noise']
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
                seg_messages = list(it.imap(self._convert_messages_out, seg, it.repeat(seg.id)))
                if seg_messages:
                    # ignore segments that come out with no messages, these are just the prior segment states that we
                    # passed it that were kicked out because they are too old
                    for msg in seg_messages:
                        yield msg
                    seg_state = seg.state
                    seg_states.append(seg_state)
                    seg_record = self._segment_record(seg_messages, seg_state)
                    logging.debug('Segmenting key %s yielding segment %s containing %s messages ' % (seg.mmsi, seg.id, len(seg_messages)))
                    yield TaggedOutput(Segment.OUTPUT_TAG_SEGMENTS, seg_record)


    def segment(self, kv, segments_map):
        key, messages = kv
        segments = segments_map.get(key, [])

        messages = sorted(messages, key=lambda msg: msg['timestamp'])
        logging.debug('Segmenting key %s sorted %s messages' % (key, len(messages)))
        for item in self._gpsdio_segment(messages, segments):
            yield item

    def expand(self, xs):
        return (
            xs | FlatMap(self.segment, AsDict(self.segments_pcoll_kv))
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
        field.type = "STRING"
        field.mode = "REQUIRED"
        schema.fields.append(field)

        field = bigquery.TableFieldSchema()
        field.name = "noise"
        field.type = "BOOLEAN"
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
