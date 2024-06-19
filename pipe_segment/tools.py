# import datetime
from datetime import date, datetime, timezone as tz

import six
from dateutil.parser import parse as dateutil_parse

EPOCH = datetime.utcfromtimestamp(0).replace(tzinfo=tz.utc)


def timestamp_from_datetime(dt):
    """Convert a datetime to a unix timestamp"""
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=tz.utc)

    return (dt - EPOCH).total_seconds()


def datetime_from_timestamp(ts):
    """Convert a unix timestamp to a datetime"""
    return datetime.fromtimestamp(ts, tz=tz.utc)


def as_timestamp(d):
    """Attempt to convert the passed parameter to a datetime.  Note that for strings this
    uses python-dateutil, which is flexible, but SLOW.  So do not use this methig if you
    are going to be parsing a billion dates.  Use the high performance methods in timestamp.py

    Returns:
        float: seconds since epoch (unix timestamp)
    """
    if d is None:
        return None

    if isinstance(d, datetime):
        return timestamp_from_datetime(d.replace(tzinfo=tz.utc))

    if isinstance(d, date):
        return timestamp_from_datetime(
            datetime.combine(d, datetime.min.time()).replace(tzinfo=tz.utc))

    if isinstance(d, (float, int)):
        return float(d)

    if isinstance(d, six.string_types):
        return as_timestamp(dateutil_parse(d))

    raise ValueError(f'Unsupported data type. Unable to convert value "{d}" to to a timestamp')
