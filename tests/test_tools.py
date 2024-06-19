import pytest
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


def test_as_timestamp():
    ts = 1704067200.0

    dt = None
    assert tools.as_timestamp(dt) is None

    dt = datetime(2024, 1, 1, tzinfo=timezone.utc)
    assert tools.as_timestamp(dt) == ts

    dt = datetime(2024, 1, 1, tzinfo=timezone.utc).date()
    assert tools.as_timestamp(dt) == ts

    assert tools.as_timestamp(ts) == ts
    assert tools.as_timestamp(int(ts)) == ts
    assert tools.as_timestamp("2024-01-01") == ts

    with pytest.raises(ValueError):
        tools.as_timestamp([])
