import datetime
import six
import pytz
from dateutil.parser import parse as dateutil_parse

EPOCH = datetime.datetime.utcfromtimestamp(0).replace(tzinfo=pytz.UTC)


def timestampFromDatetime(dt):
    """Convert a datetime to a unix timestamp"""
    return (dt - EPOCH).total_seconds()


def datetimeFromTimestamp(ts):
    """Convert a unix timestamp to a datetime"""
    return datetime.datetime.fromtimestamp(ts, tz=pytz.UTC)


def as_timestamp(d):
    """
    Attempt to convert the passed parameter to a datetime.  Note that for strings this
    uses python-dateutil, which is flexible, but SLOW.  So do not use this methig if you
    are going to be parsing a billion dates.  Use the high performance methods in timestamp.py

    return: float   seconds since epoch (unix timestamp)
    """
    if d is None:
        return None
    elif isinstance(d, datetime.datetime):
        return timestampFromDatetime(pytz.UTC.localize(d) if d.tzinfo is None else d)
        # return timestampFromDatetime(pytz.UTC.localize(d))
    elif isinstance(d, datetime.date):
        return timestampFromDatetime(
            pytz.UTC.localize(datetime.combine(d, datetime.min.time()))
        )
    elif isinstance(d, (float, int)):
        return float(d)
    elif isinstance(d, six.string_types):
        return as_timestamp(dateutil_parse(d))
    else:
        raise ValueError(
            'Unsupported data type. Unable to convert value "%s" to to a timestamp' % d
        )
