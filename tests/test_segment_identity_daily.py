import pytest
import unittest

from pipe_tools.utils.timestamp import as_timestamp

from pipe_segment.segment_identity.transforms import summarize_identifiers

class TestSegmentIdentityDaily():
    def test_summarize_identifiers_no_empties(self):
        segment = {
            "seg_id": u'338013000-2017-07-20T05:59:35.000000Z',
            "ssvid": u'338013000',
            "timestamp": as_timestamp('2018-01-01T00:00:00.000000Z'),
            "first_msg_of_day_timestamp": as_timestamp('2018-01-01T01:00:00.000000Z'),
            "last_msg_of_day_timestamp": as_timestamp('2018-01-01T02:00:00.000000Z'),
            "message_count": 100,
            "transponders": [
                { "value": u"338013000", "count": 90 },
                { "value": u"338013001", "count": 20 }
            ],
            "shipnames": [
                { "value": u"", "count": 90 },
                { "value": u"f/v boaty Mc Boatface", "count": 30 }
            ],
            "callsigns": [
                { "value": u"", "count": 90 },
                { "value": u"@@123", "count": 40 }
            ],
            "imos": [
                { "value": u"0", "count": 90 },
                { "value": u"8875956", "count": 50 }
            ]
        }

        result = summarize_identifiers(segment)

        assert result == {
            "seg_id": u'338013000-2017-07-20T05:59:35.000000Z',
            "ssvid": u'338013000',
            "timestamp": as_timestamp('2018-01-01T00:00:00.000000Z'),
            "first_timestamp": as_timestamp('2018-01-01T01:00:00.000000Z'),
            "last_timestamp": as_timestamp('2018-01-01T02:00:00.000000Z'),
            "first_pos_timestamp": as_timestamp('2018-01-01T01:00:00.000000Z'),
            "last_pos_timestamp": as_timestamp('2018-01-01T02:00:00.000000Z'),
            "msg_count": 100,
            "pos_count": 110,
            "ident_count": 120,
            "shipname": [
                { "value": u"", "count": 90 },
                { "value": u"f/v boaty Mc Boatface", "count": 30 }
            ],
            "callsign": [
                { "value": u"", "count": 90 },
                { "value": u"@@123", "count": 40 }
            ],
            "imo": [
                { "value": u"0", "count": 90 },
                { "value": u"8875956", "count": 50 }
            ],
            "n_shipname": [
                { "value": u"BOATYMCBOATFACE", "count": 30 }
            ],
            "n_callsign": [
                { "value": u"123", "count": 40 }
            ],
            "n_imo": [
                { "value": u"8875956", "count": 50 }
            ],
            "shiptype": None,
            "length": None,
            "width": None,
            "noise": False
        }

    def test_summarize_identifiers_empty_identifiers(self):
        segment = {
            "seg_id": u'338013000-2017-07-20T05:59:35.000000Z',
            "ssvid": u'338013000',
            "timestamp": as_timestamp('2018-01-01T00:00:00.000000Z'),
            "first_msg_of_day_timestamp": as_timestamp('2018-01-01T01:00:00.000000Z'),
            "last_msg_of_day_timestamp": as_timestamp('2018-01-01T02:00:00.000000Z'),
            "message_count": 100,
            "transponders": [
                { "value": u"338013000", "count": 90 },
                { "value": u"338013001", "count": 20 }
            ],
            "shipnames": [
                { "value": u"", "count": 90 },
            ],
            "callsigns": [
                { "value": u"", "count": 90 },
            ],
            "imos": [
                { "value": u"0", "count": 90 },
            ]
        }

        result = summarize_identifiers(segment)

        assert result == {
            "seg_id": u'338013000-2017-07-20T05:59:35.000000Z',
            "ssvid": u'338013000',
            "timestamp": as_timestamp('2018-01-01T00:00:00.000000Z'),
            "first_timestamp": as_timestamp('2018-01-01T01:00:00.000000Z'),
            "last_timestamp": as_timestamp('2018-01-01T02:00:00.000000Z'),
            "first_pos_timestamp": as_timestamp('2018-01-01T01:00:00.000000Z'),
            "last_pos_timestamp": as_timestamp('2018-01-01T02:00:00.000000Z'),
            "msg_count": 100,
            "pos_count": 110,
            "ident_count": 90,
            "shipname": [
                { "value": u"", "count": 90 },
            ],
            "callsign": [
                { "value": u"", "count": 90 },
            ],
            "imo": [
                { "value": u"0", "count": 90 },
            ],
            "n_shipname": None,
            "n_callsign": None,
            "n_imo": None,
            "shiptype": None,
            "length": None,
            "width": None,
            "noise": False
        }

