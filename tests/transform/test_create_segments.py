import apache_beam as beam
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to

from pipe_segment.transform.create_segments import CreateSegments


INPUTS = [
    (
        "226010660-2024-04-12T13:54:51.000000Z-1",
        [
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
        ],
    ),
    (
        "226013750-2024-04-12T00:15:39.000000Z-1",
        [
            {
                "frag_id": "226013750-2024-04-12T00:15:39.000000Z-1",
                "ssvid": "226013750",
                "timestamp": 1712880000.0,
                "first_msg_timestamp": 1712880939.0,
                "first_msg_lat": 44.556666666666665,
                "first_msg_lon": -0.24666666666666667,
                "first_msg_course": None,
                "first_msg_speed": 0.0,
                "last_msg_timestamp": 1712962840.0,
                "last_msg_lat": 44.556666666666665,
                "last_msg_lon": -0.24666666666666667,
                "last_msg_course": None,
                "last_msg_speed": 0.0,
                "msg_count": 21,
                "identities": [{"count": 1}],
                "destinations": [{"count": 1}],
                "seg_id": "226013750-2024-04-12T00:15:39.000000Z-1",
            }
        ],
    ),
]


OUTPUT = [
    {
        "frag_id": "226010660-2024-04-12T13:54:51.000000Z-1",
        "ssvid": "226010660",
        "timestamp": 1712880000.0,
        "seg_id": "226010660-2024-04-12T13:54:51.000000Z-1",
        "first_timestamp": 1712930091.0,
        "daily_msg_count": 1,
        "cumulative_msg_count": 1,
        "daily_identities": [],
        "cumulative_identities": [],
        "daily_destinations": [],
        "cumulative_destinations": [],
    },
    {
        "frag_id": "226013750-2024-04-12T00:15:39.000000Z-1",
        "ssvid": "226013750",
        "timestamp": 1712880000.0,
        "seg_id": "226013750-2024-04-12T00:15:39.000000Z-1",
        "first_timestamp": 1712880939.0,
        "daily_msg_count": 21,
        "cumulative_msg_count": 21,
        "daily_identities": [{"count": 1}],
        "cumulative_identities": [{"count": 1}],
        "daily_destinations": [{"count": 1}],
        "cumulative_destinations": [{"count": 1}],
    },
]


def test_create_segments():
    op = CreateSegments()
    assert next(op.update_msgs(INPUTS[0])) == OUTPUT[0]

    # Beam
    with TestPipeline() as p:
        pcoll = p | beam.Create(INPUTS)
        output = pcoll | op
        assert_that(output, equal_to(OUTPUT))
