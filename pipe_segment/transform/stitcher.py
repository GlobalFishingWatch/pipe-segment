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

from pipe_tools.timestamp import datetimeFromTimestamp
from pipe_tools.timestamp import timestampFromDatetime

from .stitcher_implementation import StitcherImplementation

logger = logging.getLogger(__file__)
logger.setLevel(logging.DEBUG)

class Stitch(PTransform):

    OUTPUT_TAG_MESSAGES = 'messages'

    def __init__(self, 
                 start_date,
                 end_date,
                 look_ahead,
                 stitcher_params=None, 
                 **kwargs):
        super(Stitch, self).__init__(**kwargs)
        self._stitcher = StitcherImplementation(start_date, end_date, look_ahead, 
                                                stitcher_params)


    @staticmethod
    def _convert_segment_in(seg):
        seg = dict(seg.items())
        for k in ['timestamp', 'first_msg_timestamp', 'last_msg_timestamp',
                    'first_msg_of_day_timestamp', 'last_msg_of_day_timestamp']:
            if seg[k] is not None:
                seg[k] = datetimeFromTimestamp(seg[k])
        return seg


    @staticmethod
    def _convert_track_in(track):
        track = dict(track.items())
        for k in ['timestamp']:
            if track[k] is not None:
                track[k] = datetimeFromTimestamp(track[k])
        return track

    @staticmethod
    def _convert_track_out(track):
        track = dict(track.items())
        for k in ['timestamp']:
            if track[k] is not None:
                track[k] = timestampFromDatetime(track[k])
        return track

    def stitch(self, kv):
        """Implement iterative stitching of segments into tracks

        Parameters
        ==========
        kv : (ssvid, {'tracks' : tracks, 'segments' : segments})

        tracks:  previous tracks from 1 day previous.
        segments: segments corresponding from beginning of time to 
                  +lookahead; these values are specified in make_tracks.
                  See note in segment implementation about how lookback might
                  be reduced

        Note that this may be rerun as more data becomes available, overwriting
        tracks, so tracks and segments should be filtered on the way in, or alternatively,
        queried appropriately, so that the above conditions are satisfied.

        """
        ssvid, track_segment_map = kv
        tracks = track_segment_map['tracks']
        raw_segments = track_segment_map['segments']

        segments = [self._convert_segment_in(x) for x in raw_segments]

        logger.debug('Stitching key %r with %s segments', ssvid, len(segments))

        for track in  self._stitcher.stitch(ssvid, tracks, segments):
            yield self._convert_track_out(track)

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
        add_field('timestamp', "TIMESTAMP")
        add_field('seg_ids', 'STRING', mode='REPEATED')

        return schema


