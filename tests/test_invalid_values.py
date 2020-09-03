import pytest

from pipe_segment.transform.invalid_values import filter_invalid_values

class TestInvalidData:
    def test_type_1_messages(self):
        source = [
            {"type": "AIS.1", "lat": -35, "lon": -125, "course": 112.35, "speed": 35.45, "heading": 120},
            {"type": "AIS.1", "lat": -90, "lon": -180, "course": 0, "speed": 0, "heading": 0},
            {"type": "AIS.1", "lat": -91, "lon": -181, "course": -1, "speed": -1, "heading": -1},
            {"type": "AIS.1", "lat": 91, "lon": 181, "course": 360, "speed": 102.3, "heading": 360},
            {"type": "AIS.1", "lat": 90.999997, "lon": 180.999996, "course": 359.96, "speed": 102.29, "heading": 359.5},
            {"type": "AIS.1", "lat": 90.000001, "lon": 180.000001, "course": 359.919996, "speed": 102.22999, "heading": 359.4},
            {"type": "AIS.1", "lat": None, "lon": None, "course": None, "speed": None, "heading": None},
        ]

        expected = [
            {"type": "AIS.1", "lat": -35, "lon": -125, "course": 112.35, "speed": 35.45, "heading": 120},
            {"type": "AIS.1", "lat": -90, "lon": -180, "course": 0, "speed": 0, "heading": 0},
            {"type": "AIS.1", "lat": None, "lon": None, "course": None, "speed": None, "heading": None},
            {"type": "AIS.1", "lat": None, "lon": None, "course": None, "speed": None, "heading": None},
            {"type": "AIS.1", "lat": None, "lon": None, "course": None, "speed": None, "heading": None},
            {"type": "AIS.1", "lat": 90.000001, "lon": 180.000001, "course": 359.919996, "speed": 102.22999, "heading": 359.4},
            {"type": "AIS.1", "lat": None, "lon": None, "course": None, "speed": None, "heading": None},
        ]

        actual = [filter_invalid_values(x) for x in source]

        assert expected == actual

    def test_type_2_messages(self):
        source = [
            {"type": "AIS.2", "lat": -35, "lon": -125, "course": 112.35, "speed": 35.45, "heading": 120},
            {"type": "AIS.2", "lat": -90, "lon": -180, "course": 0, "speed": 0, "heading": 0},
            {"type": "AIS.2", "lat": -91, "lon": -181, "course": -1, "speed": -1, "heading": -1},
            {"type": "AIS.2", "lat": 91, "lon": 181, "course": 360, "speed": 102.3, "heading": 360},
            {"type": "AIS.2", "lat": 90.999997, "lon": 180.999996, "course": 359.96, "speed": 102.29, "heading": 359.5},
            {"type": "AIS.2", "lat": 90.000001, "lon": 180.000001, "course": 359.919996, "speed": 102.22999, "heading": 359.4},
            {"type": "AIS.2", "lat": None, "lon": None, "course": None, "speed": None, "heading": None},
        ]

        expected = [
            {"type": "AIS.2", "lat": -35, "lon": -125, "course": 112.35, "speed": 35.45, "heading": 120},
            {"type": "AIS.2", "lat": -90, "lon": -180, "course": 0, "speed": 0, "heading": 0},
            {"type": "AIS.2", "lat": None, "lon": None, "course": None, "speed": None, "heading": None},
            {"type": "AIS.2", "lat": None, "lon": None, "course": None, "speed": None, "heading": None},
            {"type": "AIS.2", "lat": None, "lon": None, "course": None, "speed": None, "heading": None},
            {"type": "AIS.2", "lat": 90.000001, "lon": 180.000001, "course": 359.919996, "speed": 102.22999, "heading": 359.4},
            {"type": "AIS.2", "lat": None, "lon": None, "course": None, "speed": None, "heading": None},
        ]

        actual = [filter_invalid_values(x) for x in source]

        assert expected == actual

    def test_type_3_messages(self):
        source = [
            {"type": "AIS.3", "lat": -35, "lon": -125, "course": 112.35, "speed": 35.45, "heading": 120},
            {"type": "AIS.3", "lat": -90, "lon": -180, "course": 0, "speed": 0, "heading": 0},
            {"type": "AIS.3", "lat": -91, "lon": -181, "course": -1, "speed": -1, "heading": -1},
            {"type": "AIS.3", "lat": 91, "lon": 181, "course": 360, "speed": 102.3, "heading": 360},
            {"type": "AIS.3", "lat": 90.999997, "lon": 180.999996, "course": 359.96, "speed": 102.29, "heading": 359.5},
            {"type": "AIS.3", "lat": 90.000001, "lon": 180.000001, "course": 359.919996, "speed": 102.22999, "heading": 359.4},
            {"type": "AIS.3", "lat": None, "lon": None, "course": None, "speed": None, "heading": None},
        ]

        expected = [
            {"type": "AIS.3", "lat": -35, "lon": -125, "course": 112.35, "speed": 35.45, "heading": 120},
            {"type": "AIS.3", "lat": -90, "lon": -180, "course": 0, "speed": 0, "heading": 0},
            {"type": "AIS.3", "lat": None, "lon": None, "course": None, "speed": None, "heading": None},
            {"type": "AIS.3", "lat": None, "lon": None, "course": None, "speed": None, "heading": None},
            {"type": "AIS.3", "lat": None, "lon": None, "course": None, "speed": None, "heading": None},
            {"type": "AIS.3", "lat": 90.000001, "lon": 180.000001, "course": 359.919996, "speed": 102.22999, "heading": 359.4},
            {"type": "AIS.3", "lat": None, "lon": None, "course": None, "speed": None, "heading": None},
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
            {"type": "AIS.5", "imo": "0001000000", "callsign": "1234567", "shipname": "Some ship", "destination": "Some port"},
            {"type": "AIS.5", "imo": "0000999999", "callsign": "@@@@@@@", "shipname": "@@@@@@@@@@@@@@@@@@@@", "destination": "@@@@@@@@@@@@@@@@@@@@"},
            {"type": "AIS.5", "imo": "001000000", "callsign": "123456", "shipname": "Some ship", "destination": "Some port"},
            {"type": "AIS.5", "imo": None, "callsign": None, "shipname": None, "destination": None},
        ]

        expected = [
            {"type": "AIS.5", "imo": "0001000000", "callsign": "1234567", "shipname": "Some ship", "destination": "Some port"},
            {"type": "AIS.5", "imo": None, "callsign": None, "shipname": None, "destination": None},
            {"type": "AIS.5", "imo": None, "callsign": "123456", "shipname": "Some ship", "destination": "Some port"},
            {"type": "AIS.5", "imo": None, "callsign": None, "shipname": None, "destination": None},
        ]

        actual = [filter_invalid_values(x) for x in source]

        assert expected == actual

    def test_type_9_messages(self):
        source = [
            {"type": "AIS.9", "lat": -35, "lon": -125, "course": 112.35, "speed": 35.45},
            {"type": "AIS.9", "lat": -90, "lon": -180, "course": 0, "speed": 0},
            {"type": "AIS.9", "lat": -91, "lon": -181, "course": -1, "speed": -1},
            {"type": "AIS.9", "lat": 91, "lon": 181, "course": 360, "speed": 102.3},
            {"type": "AIS.9", "lat": 90.999997, "lon": 180.999996, "course": 359.96, "speed": 102.29},
            {"type": "AIS.9", "lat": 90.000001, "lon": 180.000001, "course": 359.919996, "speed": 102.22999},
            {"type": "AIS.9", "lat": None, "lon": None, "course": None, "speed": None},
        ]

        expected = [
            {"type": "AIS.9", "lat": -35, "lon": -125, "course": 112.35, "speed": 35.45},
            {"type": "AIS.9", "lat": -90, "lon": -180, "course": 0, "speed": 0},
            {"type": "AIS.9", "lat": None, "lon": None, "course": None, "speed": None},
            {"type": "AIS.9", "lat": None, "lon": None, "course": None, "speed": None},
            {"type": "AIS.9", "lat": None, "lon": None, "course": None, "speed": None},
            {"type": "AIS.9", "lat": 90.000001, "lon": 180.000001, "course": 359.919996, "speed": 102.22999},
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
            {"type": "AIS.18", "lat": -35, "lon": -125, "course": 112.35, "speed": 35.45, "heading": 120},
            {"type": "AIS.18", "lat": -90, "lon": -180, "course": 0, "speed": 0, "heading": 0},
            {"type": "AIS.18", "lat": -91, "lon": -181, "course": -1, "speed": -1, "heading": -1},
            {"type": "AIS.18", "lat": 91, "lon": 181, "course": 360, "speed": 102.3, "heading": 360},
            {"type": "AIS.18", "lat": 90.999997, "lon": 180.999996, "course": 359.96, "speed": 102.29, "heading": 359.5},
            {"type": "AIS.18", "lat": 90.000001, "lon": 180.000001, "course": 359.919996, "speed": 102.22999, "heading": 359.4},
            {"type": "AIS.18", "lat": None, "lon": None, "course": None, "speed": None, "heading": None},
        ]

        expected = [
            {"type": "AIS.18", "lat": -35, "lon": -125, "course": 112.35, "speed": 35.45, "heading": 120},
            {"type": "AIS.18", "lat": -90, "lon": -180, "course": 0, "speed": 0, "heading": 0},
            {"type": "AIS.18", "lat": None, "lon": None, "course": None, "speed": None, "heading": None},
            {"type": "AIS.18", "lat": None, "lon": None, "course": None, "speed": None, "heading": None},
            {"type": "AIS.18", "lat": None, "lon": None, "course": None, "speed": None, "heading": None},
            {"type": "AIS.18", "lat": 90.000001, "lon": 180.000001, "course": 359.919996, "speed": 102.22999, "heading": 359.4},
            {"type": "AIS.18", "lat": None, "lon": None, "course": None, "speed": None, "heading": None},
        ]

        actual = [filter_invalid_values(x) for x in source]

        assert expected == actual

    def test_type_19_messages(self):
        source = [
            {"type": "AIS.19", "lat": -35, "lon": -125, "course": 112.35, "speed": 35.45, "heading": 120},
            {"type": "AIS.19", "lat": -90, "lon": -180, "course": 0, "speed": 0, "heading": 0, "shipname": "Some vessel name"},
            {"type": "AIS.19", "lat": -91, "lon": -181, "course": -1, "speed": -1, "heading": -1, "shipname": "@@@@@@@@@@@@@@@@@@@@"},
            {"type": "AIS.19", "lat": 91, "lon": 181, "course": 360, "speed": 102.3, "heading": 360},
            {"type": "AIS.19", "lat": 90.999997, "lon": 180.999996, "course": 359.96, "speed": 102.29, "heading": 359.5},
            {"type": "AIS.19", "lat": 90.000001, "lon": 180.000001, "course": 359.919996, "speed": 102.22999, "heading": 359.4},
            {"type": "AIS.19", "lat": None, "lon": None, "course": None, "speed": None, "heading": None},
        ]

        expected = [
            {"type": "AIS.19", "lat": -35, "lon": -125, "course": 112.35, "speed": 35.45, "heading": 120},
            {"type": "AIS.19", "lat": -90, "lon": -180, "course": 0, "speed": 0, "heading": 0, "shipname": "Some vessel name"},
            {"type": "AIS.19", "lat": None, "lon": None, "course": None, "speed": None, "heading": None, "shipname": None},
            {"type": "AIS.19", "lat": None, "lon": None, "course": None, "speed": None, "heading": None},
            {"type": "AIS.19", "lat": None, "lon": None, "course": None, "speed": None, "heading": None},
            {"type": "AIS.19", "lat": 90.000001, "lon": 180.000001, "course": 359.919996, "speed": 102.22999, "heading": 359.4},
            {"type": "AIS.19", "lat": None, "lon": None, "course": None, "speed": None, "heading": None},
        ]

        actual = [filter_invalid_values(x) for x in source]

        assert expected == actual

    def test_type_21_messages(self):
        source = [
            {"type": "AIS.21", "lat": -35, "lon": -125, "shipname": "Some vessel name"},
            {"type": "AIS.21", "lat": -90, "lon": -180, "shipname": "@@@@@@@@@@@@@@@@@@@@"},
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
            {"type": "AIS.24", "callsign": "@@@@@@@", "shipname": "@@@@@@@@@@@@@@@@@@@@"},
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
            {"type": "AIS.27", "lat": -35, "lon": -125, "course": 112.35, "speed": 35.45},
            {"type": "AIS.27", "lat": -90, "lon": -180, "course": 0, "speed": 0},
            {"type": "AIS.27", "lat": -91, "lon": -181, "course": -1, "speed": -1},
            {"type": "AIS.27", "lat": 91, "lon": 181, "course": 360, "speed": 102.3},
            {"type": "AIS.27", "lat": 90.997, "lon": 180.996, "course": 359.6, "speed": 62.9},
            {"type": "AIS.27", "lat": 90.001, "lon": 180.001, "course": 359.1, "speed": 62.2999},
            {"type": "AIS.27", "lat": None, "lon": None, "course": None, "speed": None},
        ]

        expected = [
            {"type": "AIS.27", "lat": -35, "lon": -125, "course": 112.35, "speed": 35.45},
            {"type": "AIS.27", "lat": -90, "lon": -180, "course": 0, "speed": 0},
            {"type": "AIS.27", "lat": None, "lon": None, "course": None, "speed": None},
            {"type": "AIS.27", "lat": None, "lon": None, "course": None, "speed": None},
            {"type": "AIS.27", "lat": None, "lon": None, "course": None, "speed": None},
            {"type": "AIS.27", "lat": 90.001, "lon": 180.001, "course": 359.1, "speed": 62.2999},
            {"type": "AIS.27", "lat": None, "lon": None, "course": None, "speed": None},
        ]

        actual = [filter_invalid_values(x) for x in source]

        assert expected == actual
