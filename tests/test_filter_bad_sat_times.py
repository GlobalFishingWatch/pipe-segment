import pytest
from datetime import datetime
from pytz import UTC

from pipe_segment.transform.filter_bad_satellite_times import FilterBadSatelliteTimes
from pipe_segment.tools import timestampFromDatetime


class TestFilterBadSatelliteTimes:
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
    def test_make_sat_key(self, hour, offset, expected):
        tx = FilterBadSatelliteTimes(None, None, None)
        assert (
            tx.make_sat_key(
                "rcvr", timestampFromDatetime(hour.replace(tzinfo=UTC)), offset
            )
            == expected
        )

    @pytest.mark.parametrize(
        "msg,padding,expected",
        [
            (
                {
                    "receiver": "rcvr1",
                    "hour": timestampFromDatetime(datetime(2020, 1, 1, 23, tzinfo=UTC)),
                },
                1,
                ["rcvr1-2020-1-1-22", "rcvr1-2020-1-1-23", "rcvr1-2020-1-2-0"],
            ),
            (
                {
                    "receiver": "rcvr2",
                    "hour": timestampFromDatetime(datetime(2020, 1, 2, 0, tzinfo=UTC)),
                },
                1,
                ["rcvr2-2020-1-1-23", "rcvr2-2020-1-2-0", "rcvr2-2020-1-2-1"],
            ),
            (
                {
                    "receiver": "rcvr1",
                    "hour": timestampFromDatetime(datetime(2020, 1, 1, 23, tzinfo=UTC)),
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
    def test_make_offset_sat_key_set(self, msg, padding, expected):
        tx = FilterBadSatelliteTimes(None, None, bad_hour_padding=padding)
        assert list(tx.make_offset_sat_key_set(msg)) == expected

    @pytest.mark.parametrize(
        "msg, expected",
        [
            (
                {
                    "receiver": "rcvr1",
                    "timestamp": timestampFromDatetime(
                        datetime(2020, 1, 1, 23, 37, tzinfo=UTC)
                    ),
                },
                False,
            ),
            (
                {
                    "receiver": "rcvr1",
                    "timestamp": timestampFromDatetime(
                        datetime(2020, 1, 2, 0, 0, tzinfo=UTC)
                    ),
                },
                False,
            ),
            (
                {
                    "receiver": "rcvr1",
                    "timestamp": timestampFromDatetime(
                        datetime(2020, 1, 1, 22, 47, tzinfo=UTC)
                    ),
                },
                False,
            ),
            (
                {
                    "receiver": "rcvr2",
                    "timestamp": timestampFromDatetime(
                        datetime(2020, 1, 2, 0, 0, tzinfo=UTC)
                    ),
                },
                True,
            ),
            (
                {
                    "receiver": "rcvr1",
                    "timestamp": timestampFromDatetime(
                        datetime(2020, 1, 2, 1, 22, tzinfo=UTC)
                    ),
                },
                True,
            ),
            (
                {
                    "receiver": "rcvr1",
                    "timestamp": timestampFromDatetime(
                        datetime(2020, 1, 1, 21, 22, tzinfo=UTC)
                    ),
                },
                True,
            ),
        ],
    )
    def test_not_during_bad_hour(self, msg, expected):
        tx = FilterBadSatelliteTimes(None, None, bad_hour_padding=1)
        bad_hours_list = tx.make_offset_sat_key_set(
            {
                "receiver": "rcvr1",
                "hour": timestampFromDatetime(datetime(2020, 1, 1, 23, tzinfo=UTC)),
            }
        )
        bad_hours = dict((x, x) for x in bad_hours_list)
        assert tx.not_during_bad_hour(msg, bad_hours) == expected
