import pytest
import unittest
import ujson
from datetime import datetime

import apache_beam as beam
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to
from apache_beam.testing.test_pipeline import TestPipeline as _TestPipeline
# rename the class to prevent py.test from trying to collect TestPipeline as a unit test class

from pipeline.coders import JSONCoder
from pipeline.coders import timestamp2datetime
from pipeline.coders import datetime2timestamp
from pipeline.coders import Timestamp2DatetimeDoFn
from pipeline.coders import Datetime2TimestampDoFn


@pytest.mark.filterwarnings('ignore:Using fallback coder:UserWarning')
@pytest.mark.filterwarnings('ignore:The compiler package is deprecated and removed in Python 3.x.:DeprecationWarning')
class TestCoders(unittest.TestCase):

    def test_JSONCoder(self):
        records = [
            0,
            {'a': 1, 'b': 2, 'c': None},
            "test",
            None
        ]
        coder = JSONCoder()
        for r in records:
            self.assertEqual(r, coder.decode(coder.encode(r)))
            self.assertEqual(ujson.dumps(r), coder.encode(r))

    def test_datetime2timestamp(self):
        now = datetime.utcnow()

        assert timestamp2datetime(datetime2timestamp(now)) == now


    def test_Datetime2TimestampDoFn(self):
        messages = [
            {'timestamp': 1506794044.401804,'expected': 1506794044.401804}
            ]
        with _TestPipeline() as p:
            result = (
                p
                | beam.Create(messages)
                | beam.ParDo(Timestamp2DatetimeDoFn())
                | beam.ParDo(Datetime2TimestampDoFn())
            )

            assert_that(result, equal_to(messages))