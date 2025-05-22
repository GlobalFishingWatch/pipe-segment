from datetime import datetime, date

from pipe_segment.transform import util


def test_by_day():
    items = [
        {
            "timestamp": datetime(2024, 1, 2).timestamp()
        },
        {
            "timestamp": datetime(2024, 1, 1).timestamp()
        },
    ]

    expected = [
        (date(2024, 1, 1), [{'timestamp': datetime(2024, 1, 1).timestamp()}]),
        (date(2024, 1, 2), [{'timestamp': datetime(2024, 1, 2).timestamp()}])
    ]

    items_by_day = list(util.by_day(items))
    assert items_by_day == expected
