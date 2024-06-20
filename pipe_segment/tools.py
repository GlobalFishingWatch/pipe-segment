from datetime import datetime, timezone as tz

from dateutil.parser import parse as dateutil_parse


def timestamp_from_datetime(dt: datetime, tzinfo: tz = tz.utc):
    """Converts a datetime to a unix timestamp.

    Args:
        dt: the datetime to convert.
        tzinfo: timezone.
    """
    return dt.replace(tzinfo=tzinfo).timestamp()


def datetime_from_timestamp(ts: float, tzinfo: tz = tz.utc):
    """Converts a unix timestamp to a datetime.

    Args:
        ts: the timestamp to convert.
        tzinfo: timezone.
    """
    return datetime.fromtimestamp(ts, tz=tzinfo)


def timestamp_from_string(d: str):
    """Converts a sring into datetime object."""
    return timestamp_from_datetime(dateutil_parse(d))
