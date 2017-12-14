import datetime as dt
import itertools as it

from apache_beam import PTransform
from apache_beam import FlatMap
from apache_beam.pvalue import TaggedOutput
from apache_beam.io.gcp.internal.clients import bigquery

from gpsdio_segment.core import Segmentizer
from gpsdio_segment.core import BadSegment
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
        # self.stats_numeric_fields = stats_numeric_fields
        # self.stats_frequency_fields = stats_frequency_fields

    @staticmethod
    def _convert_messages_in(msg):
        msg = dict(msg)
        msg['timestamp'] = datetimeFromTimestamp(msg['timestamp'])
        return msg

    @staticmethod
    def _convert_messages_out(msg, seg_id):
        msg = JSONDict(msg)
        timestamp = timestampFromDatetime(msg['timestamp'])
        msg['timestamp'] = timestamp
        msg['seg_id'] = seg_id
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

        record = {
            "seg_id": seg_state.id,
            "mmsi": seg_state.mmsi,
            "message_count": seg_state.msg_count
        }

        for field, stats in self.stats_fields:
            stat_values = ms.field_stats(field)
            for stat in stats:
                record[self.stat_output_field_name(field, stat)] = stat_values.get(stat, None)

        return record

    def _gpsdio_segment(self, messages):

        messages = it.imap(self._convert_messages_in, messages)

        for seg in Segmentizer(messages, **self.segmenter_params):
            if isinstance(seg, BadSegment):
                seg_id = "{}-BAD".format(seg.id)
            else:
                seg_id = seg.id

            seg_messages = list(it.imap(self._convert_messages_out, seg, it.repeat(seg_id)))

            seg_record = self._segment_record(seg_messages, seg.state)
            yield TaggedOutput(Segment.OUTPUT_TAG_SEGMENTS, seg_record)

            for msg in seg_messages:
                yield msg

    def segment(self, kv):
        key, messages = kv

        messages = sorted(messages, key=lambda msg: msg['timestamp'])

        for item in self._gpsdio_segment(messages):
            yield item

    def expand(self, xs):
        return (
            xs | FlatMap(self.segment).with_outputs(self.OUTPUT_TAG_SEGMENTS, main=self.OUTPUT_TAG_MESSAGES)
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
        field.name = "mmsi"
        field.type = "INTEGER"
        field.mode = "REQUIRED"
        schema.fields.append(field)

        for field_name, stats in self.stats_fields:
            for stat_name in stats:
                field = bigquery.TableFieldSchema()
                field.name = Segment.stat_output_field_name(field_name, stat_name)
                field.type = STAT_TYPES.get(stat_name) or FIELD_TYPES.get(field_name, DEFAULT_FIELD_TYPE)
                field.mode = "NULLABLE"
                schema.fields.append(field)

        return schema
