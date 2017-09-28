import pytest
import unittest
import apache_beam as beam

from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to

from pipeline.transforms.identity import Identity


@pytest.mark.filterwarnings('ignore:Using fallback coder:UserWarning')
@pytest.mark.filterwarnings('ignore:The compiler package is deprecated and removed in Python 3.x.:DeprecationWarning')
class TestTransforms(unittest.TestCase):
    SAMPLE_DATA = [
        ('a', 1), ('b', 10), ('a', 2), ('a', 3), ('b', 20), ('c', 100)]

    def test_Identity(self):
        result = (
            TestPipeline()
            | beam.Create(self.SAMPLE_DATA)
            | Identity())

        assert_that(result, equal_to(self.SAMPLE_DATA))
        result.pipeline.run()
