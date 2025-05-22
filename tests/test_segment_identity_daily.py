from pipe_segment.segment_identity.transforms import summarize_identifiers
from pipe_segment.tools import timestamp_from_string


def build_example_segment_with_identities(daily_msg_count, daily_identities):
    """
    For the purposes of these tests, what we are interested in is in the daily
    identities array and the daily message counts of the segment. This function
    creates an example segment with everything that can come out of the
    segmenter, but lets each test provide those two fields to setup expectations.
    """
    return {
        "seg_id": "338013000-2017-07-16T19:15:55.000000Z-1",
        "frag_id": "338013000-2017-07-20T00:27:29.000000Z-1",
        "ssvid": "338013000",
        "timestamp": timestamp_from_string("2017-07-20 00:00:00.000000 UTC"),
        "first_msg_timestamp": timestamp_from_string("2017-01-01T11:00:00.00000 UTC"),
        "last_msg_timestamp": timestamp_from_string("2017-01-01T11:00:00.00000 UTC"),
        "daily_msg_count": daily_msg_count,
        "daily_identities": daily_identities,
        "daily_destinations": [{
            "count": 45,
            "destination": "FISHING GROUND"
        }],
        "first_timestamp": timestamp_from_string("2017-07-16 19:15:55.000000 UTC"),
        "cumulative_msg_count": "931",
        "cumulative_identities": [{
            "count": 243,
            "shipname": "FRIESLAND",
            "callsign": "WDE6789",
            "imo": "9310953",
            "transponder_type": "AIS-A",
            "length": "89.0",
            "width": "14.0"
        }, {
            "count": 2,
            "shipname": "FRIESLT^D I *2@  *\"",
            "callsign": "WDE6789",
            "imo": "9310953",
            "transponder_type": "AIS-A",
            "length": "505.0",
            "width": "14.0"
        }],
        "cumulative_destinations": [{
            "count": 38,
            "destination": "SUVA FIJI"
        }, {
            "count": 205,
            "destination": "FISHING GROUND"
        }, {
            "count": 2,
            "destination": "BIRHIL`\"\u0026BOUND"
        }]
    }


def expected_segment_identities(segment, **fields):
    return {
        "seg_id": segment.get("seg_id"),
        "ssvid": segment.get("ssvid"),
        "timestamp": segment.get("timestamp"),
        "first_pos_timestamp": segment.get("first_msg_timestamp"),
        "first_timestamp": segment.get("first_msg_timestamp"),
        "last_pos_timestamp": segment.get("last_msg_timestamp"),
        "last_timestamp": segment.get("last_msg_timestamp"),
        **fields,
    }


def counted_value(value, count):
    return {
        "count": count,
        "value": value,
    }


class TestSegmentIdentityDaily():
    def test_summarize_identifiers_no_noise(self):
        segment = build_example_segment_with_identities(
            daily_msg_count=295,
            daily_identities=[
                {
                    "count": 45,
                    "shipname": "FRIESLAND ONE",
                    "callsign": "0WDE6789",
                    "imo": "9310953",
                    "transponder_type": "AIS-A",
                    "length": "89.0",
                    "width": "14.0"
                }, {
                    "count": 40,
                    "shipname": "FRIESLAND ONE",
                    "callsign": "0WDE6789",
                    "imo": "9310953",
                    "transponder_type": "AIS-A",
                    "length": "89.0",
                    "width": "14.0"
                }, {
                    "count": 30,
                    "shipname": "OTHERSHIP",
                    "callsign": "WDE5000",
                    "imo": "9310965",
                    "transponder_type": "AIS-A",
                    "length": "90.0",
                    "width": "15.0"
                }]
        )

        result = summarize_identifiers(segment)

        assert result == expected_segment_identities(
            segment,
            msg_count=410,
            pos_count=295,
            ident_count=115,
            shipname=[
                counted_value("FRIESLAND ONE", 85),
                counted_value("OTHERSHIP", 30),
            ],
            callsign=[
                counted_value("0WDE6789", 85),
                counted_value("WDE5000", 30),
            ],
            imo=[
                counted_value("9310953", 85),
                counted_value("9310965", 30),
            ],
            n_shipname=[
                counted_value("FRIESLAND1", 85),
                counted_value("OTHERSHIP", 30),
            ],
            n_callsign=[
                counted_value("WDE6789", 85),
                counted_value("WDE5000", 30),
            ],
            n_imo=[
                counted_value("9310953", 85),
                counted_value("9310965", 30),
            ],
            length=[
                counted_value("89.0", 85),
                counted_value("90.0", 30),
            ],
            width=[
                counted_value("14.0", 85),
                counted_value("15.0", 30),

            ],
        )

    def test_summarize_identifiers_discarding_noise(self):
        segment = build_example_segment_with_identities(
            daily_msg_count=295,
            daily_identities=[
                {
                    "count": 45,
                    "shipname": "FRIESLAND ONE",
                    "callsign": "0WDE6789",
                    "imo": "9310953",
                    "transponder_type": "AIS-A",
                    "length": "89.0",
                    "width": "14.0"
                }, {
                    "count": 30,
                    "shipname": "000",
                    "callsign": "000",
                    "imo": "9310964",
                    "transponder_type": "AIS-A",
                    "length": None,
                    "width": None
                }]
        )

        result = summarize_identifiers(segment)

        assert result == expected_segment_identities(
            segment,
            msg_count=370,
            pos_count=295,
            ident_count=75,
            shipname=[counted_value("FRIESLAND ONE", 45), ],
            callsign=[counted_value("0WDE6789", 45), ],
            imo=[counted_value("9310953", 45), ],
            n_shipname=[counted_value("FRIESLAND1", 45), ],
            n_callsign=[counted_value("WDE6789", 45), ],
            n_imo=[counted_value("9310953", 45), ],
            length=[counted_value("89.0", 45), ],
            width=[counted_value("14.0", 45), ],
        )
