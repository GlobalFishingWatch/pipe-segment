from pipe_segment.transform.segment_state import SegmentState, identities_to_counter


ROW = {
    "seg_id": "1-a",
    "frag_id": "1-b",
    "ssvid": "1",
    "first_timestamp": 10.0,
    "cumulative_msg_count": 7,
    "cumulative_identities": [
        {"count": 3, "shipname": "ALPHA", "imo": None, "length": "100.5", "width": "20.0"},
        {"count": 4, "shipname": "BETA", "imo": None, "length": None, "width": None},
    ],
    "cumulative_destinations": [{"count": 2, "destination": "PORT A"}],
    "last_msg_timestamp": 20.0,
    "last_msg_lat": 1.5,
    "last_msg_lon": 2.5,
    "last_msg_course": None,
    "last_msg_speed": 0.0,
}


def test_identities_to_counter_converts_length_and_width_to_float():
    counter = identities_to_counter(ROW["cumulative_identities"])

    assert counter == {
        (("shipname", "ALPHA"), ("imo", None), ("length", 100.5), ("width", 20.0)): 3,
        (("shipname", "BETA"), ("imo", None), ("length", None), ("width", None)): 4,
    }


def test_identities_to_counter_keeps_the_order_of_the_records():
    records = [{"count": 1, "destination": "B"}, {"count": 2, "destination": "A"}]

    assert list(identities_to_counter(records)) == [
        (("destination", "B"),),
        (("destination", "A"),),
    ]


def test_identities_to_counter_accepts_missing_records():
    assert identities_to_counter(None) == {}


def test_segment_state_from_row():
    state = SegmentState.from_row(ROW)

    assert state.seg_id == "1-a"
    assert state.cumulative_msg_count == 7
    assert state.cumulative_destinations == {(("destination", "PORT A"),): 2}


def test_as_last_fragment_has_what_the_matcher_needs():
    assert SegmentState.from_row(ROW).as_last_fragment() == {
        "frag_id": "1-b",
        "ssvid": "1",
        "last_msg_timestamp": 20.0,
        "last_msg_lat": 1.5,
        "last_msg_lon": 2.5,
        "last_msg_course": None,
        "last_msg_speed": 0.0,
    }
