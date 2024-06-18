import apache_beam as beam
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to

from pipe_segment.schemas import message_schema
from pipe_segment.transform.whitelist_messages_segmented import WhitelistFields


FIELDNAMES = ["seg_id", "frag_id", "ssvid"]


INPUTS = [
    {
        "seg_id": "226010660-2024-04-12T13:54:51.000000Z-1",
        "frag_id": "226010660-2024-04-12T13:54:51.000000Z-1",
        "ssvid": "226010660",
        "timestamp": 1712880000.0,
        "first_msg_timestamp": 1712930091.0,
    }
]


def test_tag_with_seg_id():
    item = INPUTS[0]

    # missing keys
    op = WhitelistFields()
    assert set(op.whitelist(item).keys()).issubset(set(message_schema.fieldnames))

    op = WhitelistFields(fieldnames=FIELDNAMES)
    whitelist = op.whitelist(item)
    assert set(whitelist.keys()) == set(FIELDNAMES)

    # Beam
    with TestPipeline() as p:
        pcoll = p | beam.Create(INPUTS[0:1])
        output = pcoll | op

        assert_that(output, equal_to([whitelist]))
