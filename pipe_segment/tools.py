# import datetime
from datetime import timezone, date, datetime

import six
from dateutil.parser import parse as dateutil_parse

EPOCH = datetime.utcfromtimestamp(0).replace(tzinfo=timezone.utc)


def timestamp_from_datetime(dt):
    """Convert a datetime to a unix timestamp"""
    return (dt - EPOCH).total_seconds()


def datetime_from_timestamp(ts):
    """Convert a unix timestamp to a datetime"""
    return datetime.fromtimestamp(ts, tz=timezone.utc)


def as_timestamp(d):
    """Attempt to convert the passed parameter to a datetime.  Note that for strings this
    uses python-dateutil, which is flexible, but SLOW.  So do not use this methig if you
    are going to be parsing a billion dates.  Use the high performance methods in timestamp.py

    Returns:
        float: seconds since epoch (unix timestamp)
    """
    if d is None:
        return None
    elif isinstance(d, datetime):
        return timestamp_from_datetime(d.replace(tzinfo=timezone.utc))
    elif isinstance(d, date):
        return timestamp_from_datetime(
            datetime.combine(d, datetime.min.time()).replace(tzinfo=timezone.utc))
    elif isinstance(d, (float, int)):
        return float(d)
    elif isinstance(d, six.string_types):
        return as_timestamp(dateutil_parse(d))
    else:
        raise ValueError(
            'Unsupported data type. Unable to convert value "%s" to to a timestamp' % d
        )
