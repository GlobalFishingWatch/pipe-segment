from datetime import datetime, date

import apache_beam as beam
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to

from pipe_segment.transform.create_segment_map import CreateSegmentMap


INPUTS = [
    (
        "226010660",
        [
            {
                "frag_id": "226010660-2024-04-12T12:00:00.000000Z-1",
                "ssvid": "226010660",
                "timestamp": datetime(2024, 4, 12, 12).timestamp(),
                "first_msg_timestamp": datetime(2024, 4, 12, 12).timestamp(),
                "first_msg_lat": 45.785,
                "first_msg_lon": 4.8533333333333335,
                "first_msg_course": None,
                "first_msg_speed": 0.0,
                "last_msg_timestamp": datetime(2024, 4, 12, 12).timestamp(),
                "last_msg_lat": 45.785,
                "last_msg_lon": 4.8533333333333335,
                "last_msg_course": None,
                "last_msg_speed": 0.0,
                "msg_count": 1,
                "identities": [],
                "destinations": [],
            },
            {
                "frag_id": "226010660-2024-04-13T00:00:00.000000Z-1",
                "ssvid": "226010660",
                "timestamp": datetime(2024, 4, 13, 0).timestamp(),
                "first_msg_timestamp": datetime(2024, 4, 13, 0).timestamp(),
                "first_msg_lat": 45.785,
                "first_msg_lon": 4.8533333333333335,
                "first_msg_course": None,
                "first_msg_speed": 0.0,
                "last_msg_timestamp": datetime(2024, 4, 13, 0).timestamp(),
                "last_msg_lat": 45.785,
                "last_msg_lon": 4.8533333333333335,
                "last_msg_course": None,
                "last_msg_speed": 0.0,
                "msg_count": 1,
                "identities": [],
                "destinations": [],
            },
            {
                "frag_id": "226010660-2024-04-15T00:00:00.000000Z-1",
                "ssvid": "226010660",
                "timestamp": datetime(2024, 4, 15).timestamp(),
                "first_msg_timestamp": datetime(2024, 4, 15).timestamp(),
                "first_msg_lat": 45.785,
                "first_msg_lon": 4.8533333333333335,
                "first_msg_course": None,
                "first_msg_speed": 0.0,
                "last_msg_timestamp": datetime(2024, 4, 15).timestamp(),
                "last_msg_lat": 45.785,
                "last_msg_lon": 4.8533333333333335,
                "last_msg_course": None,
                "last_msg_speed": 0.0,
                "msg_count": 1,
                "identities": [],
                "destinations": [],
            },
        ],
    )
]

MERGE_PARAMS = dict(buffer_hours=0.5)


OUTPUT = [
    {
        "seg_id": "226010660-2024-04-12T12:00:00.000000Z-1",
        "date": date(2024, 4, 12),
        "frag_id": "226010660-2024-04-12T12:00:00.000000Z-1",
    },
    {
        "seg_id": "226010660-2024-04-12T12:00:00.000000Z-1",
        "date": date(2024, 4, 13),
        "frag_id": "226010660-2024-04-13T00:00:00.000000Z-1",
    },
    {
        "seg_id": "226010660-2024-04-15T00:00:00.000000Z-1",
        "date": date(2024, 4, 15),
        "frag_id": "226010660-2024-04-15T00:00:00.000000Z-1",
    },
]


def test_create_segment_map():
    op = CreateSegmentMap()  # No params

    op = CreateSegmentMap(MERGE_PARAMS)

    output = list(op.merge_fragments(INPUTS[0]))
    assert output == OUTPUT

    # Beam
    with TestPipeline() as p:
        pcoll = p | beam.Create(INPUTS)
        output = pcoll | op

        assert_that(output, equal_to(OUTPUT))
