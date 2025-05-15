from decimal import Decimal

from pipe_segment.transform.invalid_values import (filter_invalid_values,
                                                   float_to_fixed_point,
                                                   validate_all_fields_record)


class TestInvalidData:
    def test_type_1_messages(self):
        source = [
            {
                "type": "AIS.1",
                "lat": -35,
                "lon": -125,
                "course": 112.35,
                "speed": 35.45,
                "heading": 120,
            },
            {
                "type": "AIS.1",
                "lat": -90,
                "lon": -180,
                "course": 0,
                "speed": 0,
                "heading": 0,
            },
            {
                "type": "AIS.1",
                "lat": -91,
                "lon": -181,
                "course": -1,
                "speed": -1,
                "heading": -1,
            },
            {
                "type": "AIS.1",
                "lat": 91,
                "lon": 181,
                "course": 360,
                "speed": 102.3,
                "heading": 360,
            },
            {
                "type": "AIS.1",
                "lat": 90.999997,
                "lon": 180.999996,
                "course": 359.96,
                "speed": 102.29,
                "heading": 359.5,
            },
            {
                "type": "AIS.1",
                "lat": 90.000001,
                "lon": 180.000001,
                "course": 359.919996,
                "speed": 102.22999,
                "heading": 359.4,
            },
            {
                "type": "AIS.1",
                "lat": None,
                "lon": None,
                "course": None,
                "speed": None,
                "heading": None,
            },
        ]

        expected = [
            {
                "type": "AIS.1",
                "lat": -35,
                "lon": -125,
                "course": 112.35,
                "speed": 35.45,
                "heading": 120,
            },
            {
                "type": "AIS.1",
                "lat": -90,
                "lon": -180,
                "course": 0,
                "speed": 0,
                "heading": 0,
            },
            {
                "type": "AIS.1",
                "lat": None,
                "lon": None,
                "course": None,
                "speed": None,
                "heading": None,
            },
            {
                "type": "AIS.1",
                "lat": None,
                "lon": None,
                "course": None,
                "speed": None,
                "heading": None,
            },
            {
                "type": "AIS.1",
                "lat": None,
                "lon": None,
                "course": None,
                "speed": None,
                "heading": None,
            },
            {
                "type": "AIS.1",
                "lat": 90.000001,
                "lon": 180.000001,
                "course": 359.919996,
                "speed": 102.22999,
                "heading": 359.4,
            },
            {
                "type": "AIS.1",
                "lat": None,
                "lon": None,
                "course": None,
                "speed": None,
                "heading": None,
            },
        ]

        actual = [filter_invalid_values(x) for x in source]

        assert expected == actual

    def test_type_2_messages(self):
        source = [
            {
                "type": "AIS.2",
                "lat": -35,
                "lon": -125,
                "course": 112.35,
                "speed": 35.45,
                "heading": 120,
            },
            {
                "type": "AIS.2",
                "lat": -90,
                "lon": -180,
                "course": 0,
                "speed": 0,
                "heading": 0,
            },
            {
                "type": "AIS.2",
                "lat": -91,
                "lon": -181,
                "course": -1,
                "speed": -1,
                "heading": -1,
            },
            {
                "type": "AIS.2",
                "lat": 91,
                "lon": 181,
                "course": 360,
                "speed": 102.3,
                "heading": 360,
            },
            {
                "type": "AIS.2",
                "lat": 90.999997,
                "lon": 180.999996,
                "course": 359.96,
                "speed": 102.29,
                "heading": 359.5,
            },
            {
                "type": "AIS.2",
                "lat": 90.000001,
                "lon": 180.000001,
                "course": 359.919996,
                "speed": 102.22999,
                "heading": 359.4,
            },
            {
                "type": "AIS.2",
                "lat": None,
                "lon": None,
                "course": None,
                "speed": None,
                "heading": None,
            },
        ]

        expected = [
            {
                "type": "AIS.2",
                "lat": -35,
                "lon": -125,
                "course": 112.35,
                "speed": 35.45,
                "heading": 120,
            },
            {
                "type": "AIS.2",
                "lat": -90,
                "lon": -180,
                "course": 0,
                "speed": 0,
                "heading": 0,
            },
            {
                "type": "AIS.2",
                "lat": None,
                "lon": None,
                "course": None,
                "speed": None,
                "heading": None,
            },
            {
                "type": "AIS.2",
                "lat": None,
                "lon": None,
                "course": None,
                "speed": None,
                "heading": None,
            },
            {
                "type": "AIS.2",
                "lat": None,
                "lon": None,
                "course": None,
                "speed": None,
                "heading": None,
            },
            {
                "type": "AIS.2",
                "lat": 90.000001,
                "lon": 180.000001,
                "course": 359.919996,
                "speed": 102.22999,
                "heading": 359.4,
            },
            {
                "type": "AIS.2",
                "lat": None,
                "lon": None,
                "course": None,
                "speed": None,
                "heading": None,
            },
        ]

        actual = [filter_invalid_values(x) for x in source]

        assert expected == actual

    def test_type_3_messages(self):
        source = [
            {
                "type": "AIS.3",
                "lat": -35,
                "lon": -125,
                "course": 112.35,
                "speed": 35.45,
                "heading": 120,
            },
            {
                "type": "AIS.3",
                "lat": -90,
                "lon": -180,
                "course": 0,
                "speed": 0,
                "heading": 0,
            },
            {
                "type": "AIS.3",
                "lat": -91,
                "lon": -181,
                "course": -1,
                "speed": -1,
                "heading": -1,
            },
            {
                "type": "AIS.3",
                "lat": 91,
                "lon": 181,
                "course": 360,
                "speed": 102.3,
                "heading": 360,
            },
            {
                "type": "AIS.3",
                "lat": 90.999997,
                "lon": 180.999996,
                "course": 359.96,
                "speed": 102.29,
                "heading": 359.5,
            },
            {
                "type": "AIS.3",
                "lat": 90.000001,
                "lon": 180.000001,
                "course": 359.919996,
                "speed": 102.22999,
                "heading": 359.4,
            },
            {
                "type": "AIS.3",
                "lat": None,
                "lon": None,
                "course": None,
                "speed": None,
                "heading": None,
            },
        ]

        expected = [
            {
                "type": "AIS.3",
                "lat": -35,
                "lon": -125,
                "course": 112.35,
                "speed": 35.45,
                "heading": 120,
            },
            {
                "type": "AIS.3",
                "lat": -90,
                "lon": -180,
                "course": 0,
                "speed": 0,
                "heading": 0,
            },
            {
                "type": "AIS.3",
                "lat": None,
                "lon": None,
                "course": None,
                "speed": None,
                "heading": None,
            },
            {
                "type": "AIS.3",
                "lat": None,
                "lon": None,
                "course": None,
                "speed": None,
                "heading": None,
            },
            {
                "type": "AIS.3",
                "lat": None,
                "lon": None,
                "course": None,
                "speed": None,
                "heading": None,
            },
            {
                "type": "AIS.3",
                "lat": 90.000001,
                "lon": 180.000001,
                "course": 359.919996,
                "speed": 102.22999,
                "heading": 359.4,
            },
            {
                "type": "AIS.3",
                "lat": None,
                "lon": None,
                "course": None,
                "speed": None,
                "heading": None,
            },
        ]

        actual = [filter_invalid_values(x) for x in source]

        assert expected == actual

    def test_type_4_messages(self):
        source = [
            {"type": "AIS.4", "lat": -35, "lon": -125},
            {"type": "AIS.4", "lat": -90, "lon": -180},
            {"type": "AIS.4", "lat": -91, "lon": -181},
            {"type": "AIS.4", "lat": 91, "lon": 181},
            {"type": "AIS.4", "lat": 90.999997, "lon": 180.999996},
            {"type": "AIS.4", "lat": 90.000001, "lon": 180.000001},
            {"type": "AIS.4", "lat": None, "lon": None},
        ]

        expected = [
            {"type": "AIS.4", "lat": -35, "lon": -125},
            {"type": "AIS.4", "lat": -90, "lon": -180},
            {"type": "AIS.4", "lat": None, "lon": None},
            {"type": "AIS.4", "lat": None, "lon": None},
            {"type": "AIS.4", "lat": None, "lon": None},
            {"type": "AIS.4", "lat": 90.000001, "lon": 180.000001},
            {"type": "AIS.4", "lat": None, "lon": None},
        ]

        actual = [filter_invalid_values(x) for x in source]

        assert expected == actual

    def test_type_11_messages(self):
        source = [
            {"type": "AIS.11", "lat": -35, "lon": -125},
            {"type": "AIS.11", "lat": -90, "lon": -180},
            {"type": "AIS.11", "lat": -91, "lon": -181},
            {"type": "AIS.11", "lat": 91, "lon": 181},
            {"type": "AIS.11", "lat": 90.999997, "lon": 180.999996},
            {"type": "AIS.11", "lat": 90.000001, "lon": 180.000001},
            {"type": "AIS.11", "lat": None, "lon": None},
        ]

        expected = [
            {"type": "AIS.11", "lat": -35, "lon": -125},
            {"type": "AIS.11", "lat": -90, "lon": -180},
            {"type": "AIS.11", "lat": None, "lon": None},
            {"type": "AIS.11", "lat": None, "lon": None},
            {"type": "AIS.11", "lat": None, "lon": None},
            {"type": "AIS.11", "lat": 90.000001, "lon": 180.000001},
            {"type": "AIS.11", "lat": None, "lon": None},
        ]

        actual = [filter_invalid_values(x) for x in source]

        assert expected == actual

    def test_type_5_messages(self):
        source = [
            {
                "type": "AIS.5",
                "imo": "1",
                "callsign": "1234567",
                "shipname": "Some ship",
                "destination": "Some port",
            },
            {
                "type": "AIS.5",
                "imo": "0000999999",
                "callsign": "@@@@@@@",
                "shipname": "@@@@@@@@@@@@@@@@@@@@",
                "destination": "@@@@@@@@@@@@@@@@@@@@",
            },
            {
                "type": "AIS.5",
                "imo": "1000000",
                "callsign": "123456",
                "shipname": "Some ship",
                "destination": "Some port",
            },
            {
                "type": "AIS.5",
                "imo": None,
                "callsign": None,
                "shipname": None,
                "destination": None,
            },
            {
                "type": "AIS.5",
                "imo": "9732876",
                "callsign": None,
                "shipname": None,
                "destination": None,
            },
        ]

        expected = [
            {
                "type": "AIS.5",
                "imo": "1",
                "callsign": "1234567",
                "shipname": "Some ship",
                "destination": "Some port",
            },
            {
                "type": "AIS.5",
                "imo": "0000999999",
                "callsign": None,
                "shipname": None,
                "destination": None,
            },
            {
                "type": "AIS.5",
                "imo": "1000000",
                "callsign": "123456",
                "shipname": "Some ship",
                "destination": "Some port",
            },
            {
                "type": "AIS.5",
                "imo": None,
                "callsign": None,
                "shipname": None,
                "destination": None,
            },
            {
                "type": "AIS.5",
                "imo": "9732876",
                "callsign": None,
                "shipname": None,
                "destination": None,
            },
        ]

        actual = [filter_invalid_values(x) for x in source]

        assert expected == actual

    def test_type_9_messages(self):
        source = [
            {
                "type": "AIS.9",
                "lat": -35,
                "lon": -125,
                "course": 112.35,
                "speed": 35.45,
            },
            {"type": "AIS.9", "lat": -90, "lon": -180, "course": 0, "speed": 0},
            {"type": "AIS.9", "lat": -91, "lon": -181, "course": -1, "speed": -1},
            {"type": "AIS.9", "lat": 91, "lon": 181, "course": 360, "speed": 102.3},
            {
                "type": "AIS.9",
                "lat": 90.999997,
                "lon": 180.999996,
                "course": 359.96,
                "speed": 102.29,
            },
            {
                "type": "AIS.9",
                "lat": 90.000001,
                "lon": 180.000001,
                "course": 359.919996,
                "speed": 102.22999,
            },
            {"type": "AIS.9", "lat": None, "lon": None, "course": None, "speed": None},
        ]

        expected = [
            {
                "type": "AIS.9",
                "lat": -35,
                "lon": -125,
                "course": 112.35,
                "speed": 35.45,
            },
            {"type": "AIS.9", "lat": -90, "lon": -180, "course": 0, "speed": 0},
            {"type": "AIS.9", "lat": None, "lon": None, "course": None, "speed": None},
            {"type": "AIS.9", "lat": None, "lon": None, "course": None, "speed": None},
            {"type": "AIS.9", "lat": None, "lon": None, "course": None, "speed": None},
            {
                "type": "AIS.9",
                "lat": 90.000001,
                "lon": 180.000001,
                "course": 359.919996,
                "speed": 102.22999,
            },
            {"type": "AIS.9", "lat": None, "lon": None, "course": None, "speed": None},
        ]

        actual = [filter_invalid_values(x) for x in source]

        assert expected == actual

    def test_type_17_messages(self):
        source = [
            {"type": "AIS.17", "lat": -35, "lon": -125},
            {"type": "AIS.17", "lat": -90, "lon": -180},
            {"type": "AIS.17", "lat": -91, "lon": -181},
            {"type": "AIS.17", "lat": 91, "lon": 181},
            {"type": "AIS.17", "lat": 90.997, "lon": 180.996},
            {"type": "AIS.17", "lat": 90.001, "lon": 180.001},
            {"type": "AIS.17", "lat": None, "lon": None},
        ]

        expected = [
            {"type": "AIS.17", "lat": -35, "lon": -125},
            {"type": "AIS.17", "lat": -90, "lon": -180},
            {"type": "AIS.17", "lat": None, "lon": None},
            {"type": "AIS.17", "lat": None, "lon": None},
            {"type": "AIS.17", "lat": None, "lon": None},
            {"type": "AIS.17", "lat": 90.001, "lon": 180.001},
            {"type": "AIS.17", "lat": None, "lon": None},
        ]

        actual = [filter_invalid_values(x) for x in source]

        assert expected == actual

    def test_type_18_messages(self):
        source = [
            {
                "type": "AIS.18",
                "lat": -35,
                "lon": -125,
                "course": 112.35,
                "speed": 35.45,
                "heading": 120,
            },
            {
                "type": "AIS.18",
                "lat": -90,
                "lon": -180,
                "course": 0,
                "speed": 0,
                "heading": 0,
            },
            {
                "type": "AIS.18",
                "lat": -91,
                "lon": -181,
                "course": -1,
                "speed": -1,
                "heading": -1,
            },
            {
                "type": "AIS.18",
                "lat": 91,
                "lon": 181,
                "course": 360,
                "speed": 102.3,
                "heading": 360,
            },
            {
                "type": "AIS.18",
                "lat": 90.999997,
                "lon": 180.999996,
                "course": 359.96,
                "speed": 102.29,
                "heading": 359.5,
            },
            {
                "type": "AIS.18",
                "lat": 90.000001,
                "lon": 180.000001,
                "course": 359.919996,
                "speed": 102.22999,
                "heading": 359.4,
            },
            {
                "type": "AIS.18",
                "lat": None,
                "lon": None,
                "course": None,
                "speed": None,
                "heading": None,
            },
        ]

        expected = [
            {
                "type": "AIS.18",
                "lat": -35,
                "lon": -125,
                "course": 112.35,
                "speed": 35.45,
                "heading": 120,
            },
            {
                "type": "AIS.18",
                "lat": -90,
                "lon": -180,
                "course": 0,
                "speed": 0,
                "heading": 0,
            },
            {
                "type": "AIS.18",
                "lat": None,
                "lon": None,
                "course": None,
                "speed": None,
                "heading": None,
            },
            {
                "type": "AIS.18",
                "lat": None,
                "lon": None,
                "course": None,
                "speed": None,
                "heading": None,
            },
            {
                "type": "AIS.18",
                "lat": None,
                "lon": None,
                "course": None,
                "speed": None,
                "heading": None,
            },
            {
                "type": "AIS.18",
                "lat": 90.000001,
                "lon": 180.000001,
                "course": 359.919996,
                "speed": 102.22999,
                "heading": 359.4,
            },
            {
                "type": "AIS.18",
                "lat": None,
                "lon": None,
                "course": None,
                "speed": None,
                "heading": None,
            },
        ]

        actual = [filter_invalid_values(x) for x in source]

        assert expected == actual

    def test_type_19_messages(self):
        source = [
            {
                "type": "AIS.19",
                "lat": -35,
                "lon": -125,
                "course": 112.35,
                "speed": 35.45,
                "heading": 120,
            },
            {
                "type": "AIS.19",
                "lat": -90,
                "lon": -180,
                "course": 0,
                "speed": 0,
                "heading": 0,
                "shipname": "Some vessel name",
            },
            {
                "type": "AIS.19",
                "lat": -91,
                "lon": -181,
                "course": -1,
                "speed": -1,
                "heading": -1,
                "shipname": "@@@@@@@@@@@@@@@@@@@@",
            },
            {
                "type": "AIS.19",
                "lat": 91,
                "lon": 181,
                "course": 360,
                "speed": 102.3,
                "heading": 360,
            },
            {
                "type": "AIS.19",
                "lat": 90.999997,
                "lon": 180.999996,
                "course": 359.96,
                "speed": 102.29,
                "heading": 359.5,
            },
            {
                "type": "AIS.19",
                "lat": 90.000001,
                "lon": 180.000001,
                "course": 359.919996,
                "speed": 102.22999,
                "heading": 359.4,
            },
            {
                "type": "AIS.19",
                "lat": None,
                "lon": None,
                "course": None,
                "speed": None,
                "heading": None,
            },
        ]

        expected = [
            {
                "type": "AIS.19",
                "lat": -35,
                "lon": -125,
                "course": 112.35,
                "speed": 35.45,
                "heading": 120,
            },
            {
                "type": "AIS.19",
                "lat": -90,
                "lon": -180,
                "course": 0,
                "speed": 0,
                "heading": 0,
                "shipname": "Some vessel name",
            },
            {
                "type": "AIS.19",
                "lat": None,
                "lon": None,
                "course": None,
                "speed": None,
                "heading": None,
                "shipname": None,
            },
            {
                "type": "AIS.19",
                "lat": None,
                "lon": None,
                "course": None,
                "speed": None,
                "heading": None,
            },
            {
                "type": "AIS.19",
                "lat": None,
                "lon": None,
                "course": None,
                "speed": None,
                "heading": None,
            },
            {
                "type": "AIS.19",
                "lat": 90.000001,
                "lon": 180.000001,
                "course": 359.919996,
                "speed": 102.22999,
                "heading": 359.4,
            },
            {
                "type": "AIS.19",
                "lat": None,
                "lon": None,
                "course": None,
                "speed": None,
                "heading": None,
            },
        ]

        actual = [filter_invalid_values(x) for x in source]

        assert expected == actual

    def test_type_21_messages(self):
        source = [
            {"type": "AIS.21", "lat": -35, "lon": -125, "shipname": "Some vessel name"},
            {
                "type": "AIS.21",
                "lat": -90,
                "lon": -180,
                "shipname": "@@@@@@@@@@@@@@@@@@@@",
            },
            {"type": "AIS.21", "lat": -91, "lon": -181},
            {"type": "AIS.21", "lat": 91, "lon": 181},
            {"type": "AIS.21", "lat": 90.999997, "lon": 180.999996},
            {"type": "AIS.21", "lat": 90.000001, "lon": 180.000001},
            {"type": "AIS.21", "lat": None, "lon": None, "shipname": None},
        ]

        expected = [
            {"type": "AIS.21", "lat": -35, "lon": -125, "shipname": "Some vessel name"},
            {"type": "AIS.21", "lat": -90, "lon": -180, "shipname": None},
            {"type": "AIS.21", "lat": None, "lon": None},
            {"type": "AIS.21", "lat": None, "lon": None},
            {"type": "AIS.21", "lat": None, "lon": None},
            {"type": "AIS.21", "lat": 90.000001, "lon": 180.000001},
            {"type": "AIS.21", "lat": None, "lon": None, "shipname": None},
        ]

        actual = [filter_invalid_values(x) for x in source]

        assert expected == actual

    def test_type_24_messages(self):
        source = [
            {"type": "AIS.24", "callsign": "1234567", "shipname": "Some ship"},
            {
                "type": "AIS.24",
                "callsign": "@@@@@@@",
                "shipname": "@@@@@@@@@@@@@@@@@@@@",
            },
            {"type": "AIS.24", "callsign": "123456", "shipname": "Some ship"},
            {"type": "AIS.24", "callsign": None, "shipname": None},
        ]

        expected = [
            {"type": "AIS.24", "callsign": "1234567", "shipname": "Some ship"},
            {"type": "AIS.24", "callsign": None, "shipname": None},
            {"type": "AIS.24", "callsign": "123456", "shipname": "Some ship"},
            {"type": "AIS.24", "callsign": None, "shipname": None},
        ]

        actual = [filter_invalid_values(x) for x in source]

        assert expected == actual

    def test_type_27_messages(self):
        source = [
            {
                "type": "AIS.27",
                "lat": -35,
                "lon": -125,
                "course": 112.35,
                "speed": 35.45,
            },
            {"type": "AIS.27", "lat": -90, "lon": -180, "course": 0, "speed": 0},
            {"type": "AIS.27", "lat": -91, "lon": -181, "course": -1, "speed": -1},
            {"type": "AIS.27", "lat": 91, "lon": 181, "course": 360, "speed": 102.3},
            {
                "type": "AIS.27",
                "lat": 90.997,
                "lon": 180.996,
                "course": 359.6,
                "speed": 62.9,
            },
            {
                "type": "AIS.27",
                "lat": 90.001,
                "lon": 180.001,
                "course": 359.1,
                "speed": 62.2999,
            },
            {"type": "AIS.27", "lat": None, "lon": None, "course": None, "speed": None},
        ]

        expected = [
            {
                "type": "AIS.27",
                "lat": -35,
                "lon": -125,
                "course": 112.35,
                "speed": 35.45,
            },
            {"type": "AIS.27", "lat": -90, "lon": -180, "course": 0, "speed": 0},
            {"type": "AIS.27", "lat": None, "lon": None, "course": None, "speed": None},
            {"type": "AIS.27", "lat": None, "lon": None, "course": None, "speed": None},
            {"type": "AIS.27", "lat": None, "lon": None, "course": None, "speed": None},
            {
                "type": "AIS.27",
                "lat": 90.001,
                "lon": 180.001,
                "course": 359.1,
                "speed": 62.2999,
            },
            {"type": "AIS.27", "lat": None, "lon": None, "course": None, "speed": None},
        ]

        actual = [filter_invalid_values(x) for x in source]

        assert expected == actual

    def test_vms_type_messages(self):
        test_cases = [
            (
                "valid values",
                {
                    "type": "VMS",
                    "lat": -35,
                    "lon": -125,
                    "course": 112.35,
                    "speed": 35.45,
                },
                {
                    "type": "VMS",
                    "lat": -35,
                    "lon": -125,
                    "course": 112.35,
                    "speed": 35.45,
                },
            ),
            (
                "boundary values",
                {"type": "VMS", "lat": -90, "lon": -180, "course": 0, "speed": 0},
                {"type": "VMS", "lat": -90, "lon": -180, "course": 0, "speed": 0},
            ),
            (
                "invalid negative values",
                {"type": "VMS", "lat": -91, "lon": -181, "course": -1, "speed": -1},
                {"type": "VMS", "lat": None, "lon": None, "course": None, "speed": None},
            ),
            (
                "invalid positive values",
                {"type": "VMS", "lat": 91, "lon": 181, "course": 360, "speed": 102.3},
                {"type": "VMS", "lat": None, "lon": None, "course": None, "speed": 102.3},
            ),
            (
                "lat/lon out of bounds",
                {
                    "type": "VMS",
                    "lat": 90.997,
                    "lon": 180.996,
                    "course": 359.6,
                    "speed": 62.9,
                },
                {
                    "type": "VMS",
                    "lat": None,
                    "lon": None,
                    "course": 359.6,
                    "speed": 62.9,
                },
            ),
            (
                "lat/lon slightly out of bounds",
                {
                    "type": "VMS",
                    "lat": 90.001,
                    "lon": 180.001,
                    "course": 359.1,
                    "speed": 62.2999,
                },
                {
                    "type": "VMS",
                    "lat": None,
                    "lon": None,
                    "course": 359.1,
                    "speed": 62.2999,
                },
            ),
            (
                "all None",
                {"type": "VMS", "lat": None, "lon": None, "course": None, "speed": None},
                {"type": "VMS", "lat": None, "lon": None, "course": None, "speed": None},
            ),
            (
                "zero lat/lon",
                {
                    "type": "VMS",
                    "lat": 0,
                    "lon": 0,
                    "course": 7,
                    "speed": 12,
                },
                {
                    "type": "VMS",
                    "lat": None,
                    "lon": None,
                    "course": 7,
                    "speed": 12,
                },
            ),
            (
                "zero lat/lon",
                {
                    "type": "VMS",
                    "lat": 0.0001,
                    "lon": 0.0001,
                    "course": 7,
                    "speed": 12,
                },
                {
                    "type": "VMS",
                    "lat": 0.0001,
                    "lon": 0.0001,
                    "course": 7,
                    "speed": 12,
                },
            ),
        ]

        for label, source, expected in test_cases:
            actual = filter_invalid_values(source)
            assert actual == expected, f"Failed test case: {label}"


def test_no_validator():
    element = {"type": "DUMMY"}

    assert filter_invalid_values(element) == element


def test_validate_all_fields_record():
    expected = {
        "lat": None,
        "lon": None,
    }
    validator = validate_all_fields_record(["lat", "lon"], lambda x: x == 0)

    assert validator({"lat": 0, "lon": 0}) == {"lat": None, "lon": None}, (
        "Both lat and lon are zero"
    )
    assert validator({"lat": 10, "lon": 0}) == {"lat": 10, "lon": 0}, (
        "Only lon is zero"
    )
    assert validator({"lat": 0, "lon": 10}) == {"lat": 0, "lon": 10}, (
        "Only lat is zero"
    )
    assert validator({"lat": None, "lon": 10}) == {"lat": None, "lon": 10}, (
        "Lat is None"
    )
    assert validator({"lat": 10, "lon": None}) == {"lat": 10, "lon": None}, (
        "Lon is None"
    )
    assert validator({"lat": None, "lon": None}) == {"lat": None, "lon": None}, (
        "Both lat and lon are None"
    )
    assert validator({"lat": 0, "lon": 0, "course": 0}) == expected, (
        "Extra field is removed from the result"
    )


def test_float_to_fixed_point():
    # Basic rounding
    assert float_to_fixed_point(120.034, 1) == Decimal("120.0")
    assert float_to_fixed_point(120.034, 2) == Decimal("120.03")
    assert float_to_fixed_point(120.036, 2) == Decimal("120.04")
    # No decimal places
    assert float_to_fixed_point(123.99, 0) == Decimal("124")
    assert float_to_fixed_point(123.01, 0) == Decimal("123")
    # Negative numbers
    assert float_to_fixed_point(-45.678, 1) == Decimal("-45.7")
    assert float_to_fixed_point(-45.678, 2) == Decimal("-45.68")
    # Zero
    assert float_to_fixed_point(0, 2) == Decimal("0.00")
    # Large precision
    assert float_to_fixed_point(1.234567, 5) == Decimal("1.23457")
    # Already a Decimal
    assert float_to_fixed_point(Decimal("1.2345"), 3) == Decimal("1.234")
    # Integer input
    assert float_to_fixed_point(5, 2) == Decimal("5.00")
