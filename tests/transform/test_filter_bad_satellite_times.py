import pytest
from datetime import datetime, timezone as tz

import apache_beam as beam
from apache_beam.testing.test_pipeline import TestPipeline

from pipe_segment.transform.filter_bad_satellite_times import FilterBadSatelliteTimes
from pipe_segment.tools import timestamp_from_datetime


@pytest.mark.parametrize(
    "hour,offset,expected",
    [
        (datetime(2020, 1, 1, 23), 0, "rcvr-2020-1-1-23"),
        (datetime(2020, 1, 1, 23), 1, "rcvr-2020-1-2-0"),
        (datetime(2020, 1, 1, 23), 2, "rcvr-2020-1-2-1"),
        (datetime(2020, 1, 1, 23), -1, "rcvr-2020-1-1-22"),
        (datetime(2020, 1, 2, 0), 0, "rcvr-2020-1-2-0"),
        (datetime(2020, 1, 2, 0), -1, "rcvr-2020-1-1-23"),
    ],
)
def test_make_sat_key(hour, offset, expected):
    tx = FilterBadSatelliteTimes(None, None, None)
    assert (
        tx.make_sat_key("rcvr", timestamp_from_datetime(hour.replace(tzinfo=tz.utc)), offset)
        == expected
    )


@pytest.mark.parametrize(
    "msg,padding,expected",
    [
        (
            {
                "receiver": "rcvr1",
                "hour": timestamp_from_datetime(datetime(2020, 1, 1, 23, tzinfo=tz.utc)),
            },
            1,
            ["rcvr1-2020-1-1-22", "rcvr1-2020-1-1-23", "rcvr1-2020-1-2-0"],
        ),
        (
            {
                "receiver": "rcvr2",
                "hour": timestamp_from_datetime(datetime(2020, 1, 2, 0, tzinfo=tz.utc)),
            },
            1,
            ["rcvr2-2020-1-1-23", "rcvr2-2020-1-2-0", "rcvr2-2020-1-2-1"],
        ),
        (
            {
                "receiver": "rcvr1",
                "hour": timestamp_from_datetime(datetime(2020, 1, 1, 23, tzinfo=tz.utc)),
            },
            2,
            [
                "rcvr1-2020-1-1-21",
                "rcvr1-2020-1-1-22",
                "rcvr1-2020-1-1-23",
                "rcvr1-2020-1-2-0",
                "rcvr1-2020-1-2-1",
            ],
        ),
    ],
)
def test_make_offset_sat_key_set(msg, padding, expected):
    tx = FilterBadSatelliteTimes(None, None, bad_hour_padding=padding)
    assert list(tx.make_offset_sat_key_set(msg)) == expected


@pytest.mark.parametrize(
    "msg, expected",
    [
        (
            {
                "receiver": "rcvr1",
                "timestamp": timestamp_from_datetime(
                    datetime(2020, 1, 1, 23, 37, tzinfo=tz.utc)
                ),
            },
            False,
        ),
        (
            {
                "receiver": "rcvr1",
                "timestamp": timestamp_from_datetime(
                    datetime(2020, 1, 2, 0, 0, tzinfo=tz.utc)
                ),
            },
            False,
        ),
        (
            {
                "receiver": "rcvr1",
                "timestamp": timestamp_from_datetime(
                    datetime(2020, 1, 1, 22, 47, tzinfo=tz.utc)
                ),
            },
            False,
        ),
        (
            {
                "receiver": "rcvr2",
                "timestamp": timestamp_from_datetime(
                    datetime(2020, 1, 2, 0, 0, tzinfo=tz.utc)
                ),
            },
            True,
        ),
        (
            {
                "receiver": "rcvr1",
                "timestamp": timestamp_from_datetime(
                    datetime(2020, 1, 2, 1, 22, tzinfo=tz.utc)
                ),
            },
            True,
        ),
        (
            {
                "receiver": "rcvr1",
                "timestamp": timestamp_from_datetime(
                    datetime(2020, 1, 1, 21, 22, tzinfo=tz.utc)
                ),
            },
            True,
        ),
    ],
)
def test_not_during_bad_hour(msg, expected):
    tx = FilterBadSatelliteTimes(None, None, bad_hour_padding=1)
    bad_hours_list = tx.make_offset_sat_key_set({
        "receiver": "rcvr1",
        "hour": timestamp_from_datetime(datetime(2020, 1, 1, 23, tzinfo=tz.utc)),
    })

    bad_hours = dict((x, x) for x in bad_hours_list)
    assert tx.not_during_bad_hour(msg, bad_hours) == expected


def test_beam():
    satellite_offsets = [
        {
            "receiver": "FM130",
            "dt": -3549.536305759878,
            "pings": 104,
            "avg_distance_from_sat_km": None,
            "med_dist_from_sat_km": None,
            "hour": 1712912400.0,
        },
        {
            "receiver": "FM130",
            "dt": -3505.5649172769745,
            "pings": 1240,
            "avg_distance_from_sat_km": None,
            "med_dist_from_sat_km": None,
            "hour": 1712926800.0,
        },
        {
            "receiver": "FM130",
            "dt": -3400.2222971353985,
            "pings": 4010,
            "avg_distance_from_sat_km": None,
            "med_dist_from_sat_km": None,
            "hour": 1712930400.0,
        },
        {
            "receiver": "FM130",
            "dt": -3421.456625837142,
            "pings": 5724,
            "avg_distance_from_sat_km": None,
            "med_dist_from_sat_km": None,
            "hour": 1712941200.0,
        },
    ]

    messages = [
        {
            "timestamp": 1712932069.0,
            "msgid": "438dd53e-8c0c-a148-0acb-b59f29d0b88c",
            "source": "exactearth",
            "type": "AIS.27",
            "ssvid": "226010660",
            "lon": 4.84,
            "lat": 45.76,
            "speed": 0.0,
            "course": None,
            "heading": None,
            "shipname": None,
            "callsign": None,
            "destination": None,
            "imo": None,
            "shiptype": None,
            "receiver_type": "satellite",
            "receiver": "1161",
            "length": None,
            "width": None,
            "status": 0,
            "class_b_cs_flag": None,
        },
        {
            "timestamp": 1712931889.0,
            "msgid": "55f94c90-95ba-6541-4ebb-c6788642c0f1",
            "source": "exactearth",
            "type": "AIS.27",
            "ssvid": "226010660",
            "lon": 4.84,
            "lat": 45.763333333333335,
            "speed": 6.0,
            "course": None,
            "heading": None,
            "shipname": None,
            "callsign": None,
            "destination": None,
            "imo": None,
            "shiptype": None,
            "receiver_type": "satellite",
            "receiver": "1161",
            "length": None,
            "width": None,
            "status": 0,
            "class_b_cs_flag": None,
        },
    ]
    with TestPipeline() as p:
        pcoll_sat_offsets = p | "CreateSatOffsets" >> beam.Create(satellite_offsets)

        input_pcoll = p | beam.Create(messages)
        input_pcoll | FilterBadSatelliteTimes(pcoll_sat_offsets, 30, 1)
