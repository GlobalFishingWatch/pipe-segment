import pytest
from datetime import date

import apache_beam as beam
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to

from pipe_segment.transform.tag_with_seg_id import TagWithSegId


INPUTS = [
    (
        "226010660-2024-04-12T13:54:51.000000Z-1",
        {
            "segmap": [
                {
                    "seg_id": "226010660-2024-04-12T13:54:51.000000Z-1",
                    "date": date(2024, 4, 12),
                    "frag_id": "226010660-2024-04-12T13:54:51.000000Z-1",
                }
            ],
            "target": [
                {
                    "frag_id": "226010660-2024-04-12T13:54:51.000000Z-1",
                    "ssvid": "226010660",
                    "timestamp": 1712880000.0,
                    "first_msg_timestamp": 1712930091.0,
                    "first_msg_lat": 45.785,
                    "first_msg_lon": 4.8533333333333335,
                    "first_msg_course": None,
                    "first_msg_speed": 0.0,
                    "last_msg_timestamp": 1712930091.0,
                    "last_msg_lat": 45.785,
                    "last_msg_lon": 4.8533333333333335,
                    "last_msg_course": None,
                    "last_msg_speed": 0.0,
                    "msg_count": 1,
                    "identities": [],
                    "destinations": [],
                }
            ],
        },
    ),
    (
        "226010660-2024-04-12T13:54:51.000000Z-1",
        {
            "segmap": [
                {
                    "seg_id": None,
                }
            ],
        },
    ),
]

OUTPUTS = [
    {
        "frag_id": "226010660-2024-04-12T13:54:51.000000Z-1",
        "ssvid": "226010660",
        "timestamp": 1712880000.0,
        "first_msg_timestamp": 1712930091.0,
        "first_msg_lat": 45.785,
        "first_msg_lon": 4.8533333333333335,
        "first_msg_course": None,
        "first_msg_speed": 0.0,
        "last_msg_timestamp": 1712930091.0,
        "last_msg_lat": 45.785,
        "last_msg_lon": 4.8533333333333335,
        "last_msg_course": None,
        "last_msg_speed": 0.0,
        "msg_count": 1,
        "identities": [],
        "destinations": [],
        "seg_id": "226010660-2024-04-12T13:54:51.000000Z-1",
    }
]


def test_tag_with_seg_id():
    op = TagWithSegId()

    item = INPUTS[0]
    tagged_items = list(op.tag_msgs(item))
    assert tagged_items == OUTPUTS

    item = INPUTS[1]
    with pytest.raises(ValueError):
        list(op.tag_msgs(item))

    # Beam
    with TestPipeline() as p:
        pcoll = p | beam.Create([INPUTS[0]])
        output = pcoll | op

        assert_that(output, equal_to(OUTPUTS))
