import pytest
import unittest
import ujson
import apache_beam as beam

from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to

from pipeline.coders import JSONCoder


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

