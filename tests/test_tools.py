from datetime import datetime, timezone

from pipe_segment import tools


def test_timestamp_from_datetime():
    ts = 1704067200.0

    dt = datetime(2024, 1, 1, tzinfo=timezone.utc)
    assert tools.timestamp_from_datetime(dt) == ts

    dt = datetime(2024, 1, 1)
    assert tools.timestamp_from_datetime(dt) == ts


def test_datetime_from_timestamp():
    dt = datetime(2024, 1, 1, tzinfo=timezone.utc)

    ts = 1704067200.0
    assert tools.datetime_from_timestamp(ts) == dt


def test_timestamp_from_string():
    ts = 1704067200.0

    assert tools.timestamp_from_string("2024-01-01") == ts


def test_list_of_days():
    start_date = "2024-01-01"
    end_date = "2024-01-05"
    expected = ['2024-01-01', '2024-01-02', '2024-01-03', '2024-01-04']

    start_dt = tools.datetime_from_string(start_date)
    end_dt = tools.datetime_from_string(end_date)

    days = tools.list_of_days(start_dt, end_dt)
    days_iso = [x.date().isoformat() for x in list(days)]

    assert days_iso == expected
