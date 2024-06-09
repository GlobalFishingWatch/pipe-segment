import datetime
import itertools

import apache_beam as beam
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to

from pipe_segment.transform.create_segment_map import CreateSegmentMap


INPUTS = [
    (
        '226010660', [{
            'frag_id': '226010660-2024-04-12T13:54:51.000000Z-1',
            'ssvid': '226010660',
            'timestamp': 1712880000.0,
            'first_msg_timestamp': 1712930091.0,
            'first_msg_lat': 45.785,
            'first_msg_lon': 4.8533333333333335,
            'first_msg_course': None,
            'first_msg_speed': 0.0,
            'last_msg_timestamp': 1712930091.0,
            'last_msg_lat': 45.785,
            'last_msg_lon': 4.8533333333333335,
            'last_msg_course': None,
            'last_msg_speed': 0.0,
            'msg_count': 1,
            'identities': [],
            'destinations': []}]),
    (
        '226013750', [{
            'frag_id': '226013750-2024-04-12T00:15:39.000000Z-1',
            'ssvid': '226013750',
            'timestamp': 1712880000.0,
            'first_msg_timestamp': 1712880939.0,
            'first_msg_lat': 44.556666666666665,
            'first_msg_lon': -0.24666666666666667,
            'first_msg_course': None,
            'first_msg_speed': 0.0,
            'last_msg_timestamp': 1712962840.0,
            'last_msg_lat': 44.556666666666665,
            'last_msg_lon': -0.24666666666666667,
            'last_msg_course': None,
            'last_msg_speed': 0.0,
            'msg_count': 21,
            'identities': [],
            'destinations': []}])
]

MERGE_PARAMS = dict(buffer_hours=0.5)


OUTPUT = [
    {
        'seg_id': '226010660-2024-04-12T13:54:51.000000Z-1',
        'date': datetime.date(2024, 4, 12),
        'frag_id': '226010660-2024-04-12T13:54:51.000000Z-1'
    },
    {
        'seg_id': '226013750-2024-04-12T00:15:39.000000Z-1',
        'date': datetime.date(2024, 4, 12),
        'frag_id': '226013750-2024-04-12T00:15:39.000000Z-1'
    }
]


def test_create_segment_map():
    op = CreateSegmentMap(MERGE_PARAMS)

    output = list(itertools.chain.from_iterable(map(list, (map(op.merge_fragments, INPUTS)))))
    assert output == OUTPUT

    # Beam
    with TestPipeline() as p:
        pcoll = p | beam.Create(INPUTS)
        output = pcoll | op

        assert_that(output, equal_to(OUTPUT))
