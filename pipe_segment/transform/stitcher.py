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

from gpsdio_segment.stitcher import Stitcher

logger = logging.getLogger(__file__)
logger.setLevel(logging.DEBUG)

class Stitch(PTransform):

    OUTPUT_TAG_MESSAGES = 'messages'

    def __init__(self, stitcher_params=None, **kwargs):
        super(Stitch, self).__init__(**kwargs)
        self._stitcher_params = stitcher_params or {}

    @staticmethod
    def _convert_segment_in(seg):
        seg = dict(seg.items())
        for k in ['timestamp', 'first_msg_timestamp', 'last_msg_timestamp',
                    'first_msg_of_day_timestamp', 'last_msg_of_day_timestamp']:
            if seg[k] is not None:
                seg[k] = datetimeFromTimestamp(seg[k])
        return seg

    def stitch(self, kv):
        ssvid, raw_segments = kv

        segments = [self._convert_segment_in(x) for x in raw_segments]

        logger.debug('Stitching key %r with %s segments', ssvid, len(segments))

        stitcher = Stitcher(**self._stitcher_params)

        for raw_track in stitcher.create_tracks(segments):
            track = [x['seg_id'] for x in raw_track]
            track_id = track[0]
            yield {'ssvid' : ssvid, 'track_id' : track_id, 'seg_ids' : track}

    def expand(self, xs):
        return (
            xs | FlatMap(self.stitch)
        )

    @property
    def track_schema(self):
        schema = bigquery.TableSchema()

        def add_field(name, field_type, mode='REQUIRED'):
            field = bigquery.TableFieldSchema()
            field.name = name
            field.type = field_type
            field.mode = mode
            schema.fields.append(field)

        add_field('ssvid', 'STRING')
        add_field('track_id', 'STRING')
        add_field('seg_ids', 'STRING', mode='REPEATED')

        return schema


