import pytest
import posixpath as pp
import newlinejson as nlj

from apache_beam.testing.test_pipeline import TestPipeline as _TestPipeline
# rename the class to prevent py.test from trying to collect TestPipeline as a unit test class

import apache_beam as beam
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to
from apache_beam.testing.util import open_shards
from apache_beam import Map
from apache_beam import GroupByKey
from apache_beam import FlatMap

from datetime import datetime, date, timedelta

from pipe_tools.coders import JSONDictCoder

from pipe_segment.__main__ import run as  pipe_segment_run
from pipe_segment.transform.stitcher_implementation import StitcherImplementation, Track, Signature, BasicMessage
from pipe_segment.pipeline import SegmentPipeline



class TestStitcherImplementation():

    def _test_creation(self):
        today = date.today()
        last_week = today - timedelta(days=7)
        last_year = today - timedelta(days=365)
        return StitcherImplementation(
                start_date=last_year,
                end_date=last_week,
                look_ahead=7,
                look_back=7
            )

    def test_creation(self):
        imp = self._test_creation()

    def test_reconstitute_tracks(self):
        raw_tracks = [
            {'track_id' : 'track-1',
             'ssvid' : '123456789',
             'seg_ids' : ['seg-1', 'seg-2'],
             'count' : 10,
             'decayed_count' : 7.6,
             'is_active' : True,
             'transponders' : [{'value' : 'A',  'count' : 10}, {'value' : 'B',  'count' : 0}],
             'shipnames' : [{'value' : 'gone_fishin', 'count' : 5}, {'value' : 'gf', 'count' : 1}],
             'callsigns' : [],
             'destinations' : [],
             'lengths' : [],
             'widths' : [],
             'imos' : [],
             'last_msg_timestamp' : datetime(2018, 8, 8, 8, 8, 8),
             'last_msg_lat' : 12,
             'last_msg_lon' : 5,
             'last_msg_course' : 45,
             'last_msg_speed' : 7
             }
            ]
        imp = self._test_creation()
        [track] = imp.reconstitute_tracks(raw_tracks)
        assert track == Track(id='track-1', seg_ids=['seg-1', 'seg-2'], count=10, decayed_count=7.6,
                              is_active=True, signature=Signature(transponders={'A': 10, 'B': 0}, shipnames={'gone_fishin': 5, 'gf': 1},
                              callsigns={}, imos={}, destinations={}, lengths={}, widths={}), parent_track=None,
                              last_msg=BasicMessage(ssvid='123456789', timestamp= datetime(2018, 8, 8, 8, 8, 8), lat=12, lon=5,
                              course=45, speed=7))



