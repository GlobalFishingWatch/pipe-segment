import pytest

from datetime import datetime as dt
from dateutil.parser import parse
from pipe_segment.segment_identity.transforms import summarize_identifiers

def as_timestamp(dt):
    return parse(dt).timestamp()

class TestSegmentIdentityDaily():
    def test_summarize_identifiers_no_empties(self):
        segment = {
            "seg_id": "338013000-2017-07-16T19:15:55.000000Z-1",
            "frag_id": "338013000-2017-07-20T00:27:29.000000Z-1",
            "ssvid": "338013000",
            "timestamp": as_timestamp("2017-07-20 00:00:00.000000 UTC"),
            "daily_msg_count": 295,
            "daily_identities": [{
                "count": 45,
                "shipname": "FRIESLAND",
                "callsign": "WDE6789",
                "imo": "9310953",
                "transponder_type": "AIS-A",
                "length": "89.0",
                "width": "14.0"
            }],
            "daily_destinations": [{
                "count": 45,
                "destination": "FISHING GROUND"
            }],
            "first_timestamp": as_timestamp("2017-07-16 19:15:55.000000 UTC"),
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

        result = summarize_identifiers(segment)

        assert result == {
            "callsign": [ {
                "count": 45,
                "value": "WDE6789"
            }],
            "first_pos_timestamp": None,
            "first_timestamp": None,
            "ident_count": 45,
            "imo": [ {
                "count": 45,
                "value": "9310953"
            }],
            "last_pos_timestamp": None,
            "last_timestamp": None,
            "length": [ {
                "count": 45,
                "value": "89.0"
            }],
            "msg_count": 340,
            "n_callsign": [ {
                "count": 45,
                "value": "WDE6789"
            }],
            "n_imo": [ {
                "count": 45,
                "value": "9310953"
            }],
            "n_shipname": [ {
                "count": 45,
                "value": "FRIESLAND"
            }],
            "noise": False,
            "pos_count": 295,
            "seg_id": "338013000-2017-07-16T19:15:55.000000Z-1",
            "shipname": [ {
                "count": 45,
                "value": "FRIESLAND"
            }],
            "shiptype": None,
            "ssvid": "338013000",
            "timestamp": 1500508800.0,
            "width": [ {
                "count": 45,
                "value": "14.0"
            }]
        }


    def test_summarize_identifiers_empty_identifiers(self):
        segment = {
            "seg_id": "338013000-2017-07-16T19:15:55.000000Z-1",
            "frag_id": "338013000-2017-07-20T00:27:29.000000Z-1",
            "ssvid": "338013000",
            "timestamp": as_timestamp("2017-07-20 00:00:00.000000 UTC"),
            "daily_msg_count": 295,
            "daily_identities": [{
                "count": 45,
                "shipname": "",
                "callsign": "",
                "imo": "",
                "transponder_type": "AIS-A",
                "length": "89.0",
                "width": "14.0"
            }],
            "daily_destinations": [{
                "count": 45,
                "destination": "FISHING GROUND"
            }],
            "first_timestamp": as_timestamp("2017-07-16 19:15:55.000000 UTC"),
            "cumulative_msg_count": "931",
            "cumulative_identities": [{
                "count": 243,
                "shipname": "",
                "callsign": "",
                "imo": "",
                "transponder_type": "AIS-A",
                "length": "89.0",
                "width": "14.0"
            }, {
                "count": 2,
                "shipname": "",
                "callsign": "",
                "imo": "",
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

        result = summarize_identifiers(segment)

        assert result == {
            "callsign": [ {
                "count": 45,
                "value": ""
            } ],
            "first_pos_timestamp": None,
            "first_timestamp": None,
            "ident_count": 45,
            "imo": [ {
                "count": 45,
                "value": ""
            } ],
            "last_pos_timestamp": None,
            "last_timestamp": None,
            "length": [ {
                "count": 45,
                "value": "89.0"
            } ],
            "msg_count": 340,
            "n_callsign": None,
            "n_imo": None,
            "n_shipname": None,
            "noise": False,
            "pos_count": 295,
            "seg_id": "338013000-2017-07-16T19:15:55.000000Z-1",
            "shipname": [ {
                "count": 45,
                "value": ""
            } ],
            "shiptype": None,
            "ssvid": "338013000",
            "timestamp": 1500508800.0,
            "width": [ {
                "count": 45,
                "value": "14.0"
            } ]
        }

