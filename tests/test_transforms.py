import pytest
import unittest
from copy import deepcopy
from datetime import datetime
import pytz

import apache_beam as beam

from apache_beam.testing.test_pipeline import TestPipeline as _TestPipeline
# rename the class to prevent py.test from trying to collect TestPipeline as a unit test class

from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to
from apache_beam.testing.util import BeamAssertException
from pipe_tools.timestamp import timestampFromDatetime
from pipe_tools.timestamp import datetimeFromTimestamp

from pipe_segment.transform import Segment

from gpsdio.schema import datetime2str



@pytest.mark.filterwarnings('ignore:Using fallback coder:UserWarning')
@pytest.mark.filterwarnings('ignore:The compiler package is deprecated and removed in Python 3.x.:DeprecationWarning')
class TestTransforms(unittest.TestCase):
    t = timestampFromDatetime(datetime(2017,1,1,0,0,0, tzinfo=pytz.UTC))

    SAMPLE_DATA = [
        (1, [{'ssvid': 1, 'timestamp': t + 0}]),
        (1, [{'ssvid': 1, 'timestamp': t + 1}]),
        (1, [{'ssvid': 1, 'timestamp': t + 2}]),
        (2, [{'ssvid': 2, 'timestamp': t + 0}]),
        (2, [{'ssvid': 2, 'timestamp': t + 1}]),
        (3, [{'ssvid': 3, 'timestamp': t + 0}]),
    ]

    def test_Segment(self):
        def _seg_id_from_message(msg):
            ts = msg['timestamp']
            if not isinstance(ts, datetime):
                ts = datetimeFromTimestamp(ts)
            return '{}-{}'.format(msg['ssvid'], datetime2str(ts))

        def _expected (row):
            row = row[1][0]      # strip off the ssvid key
            row['seg_id'] = _seg_id_from_message(row)   # add seg_id
            return row

        def valid_segment():
            def _is_valid(segments):
                for seg in segments:
                    assert seg['seg_id'].startswith(str(seg['ssvid']))
                    assert seg['message_count'] == 1
                    assert seg['timestamp_count'] == 1
                return True

            return _is_valid

        expected_messages = map(_expected, deepcopy(self.SAMPLE_DATA))

        with _TestPipeline() as p:
            segmented = (
                p
                | beam.Create(self.SAMPLE_DATA)
                | "Segment" >> Segment())

            messages = segmented['messages']
            assert_that(messages, equal_to(expected_messages))

            segments = segmented[Segment.OUTPUT_TAG_SEGMENTS]
            assert_that(segments, valid_segment(), label='expected_segments')


