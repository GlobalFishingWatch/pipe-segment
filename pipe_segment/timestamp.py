from datetime import date, datetime
import udatetime
import pytz
import six
from dateutil.parser import parse as dateutil_parse
from apache_beam.utils.timestamp import Timestamp

import apache_beam as beam
from apache_beam.transforms.window import TimestampedValue

EPOCH = udatetime.utcfromtimestamp(0)
SECONDS_IN_DAY = 60 * 60 * 24

# This is the format the dataflow uses for TIMESTAMP fields returned from BigQuery
BEAM_BQ_TIMESTAMP_FORMAT = (
    "%Y-%m-%d %H:%M:%S.%f UTC"  # this is the format bigquery uses for I/O
)

# This is a much better string format for datetimes that is supported by udatetime
# Note that %z is not supported in pythng 2.7 for udatetime.strptime()
RFC3339_TIMESTAMP_FORMAT = "%Y-%m-%dT%H:%M:%S.%f%z"

DATE_FORMAT = "%Y-%m-%d"

"""
Timestamp tools

Convert between datetime, unix timestamp and various string represetnations

Dataflow BigQuery Timestamp Issues
----------------------------------

When you use the native BigQuery readers in Apache Beam Python SDK, beam transforms the float value
that BigQuery returns for the timestamp into a string "%Y-%m-%d %H:%M:%S.%f UTC"

In theory, you could supply a coder that would let you change this behavior, but the coder parameter
for BigQueryReader seems to be ignored in the python SDK, so for now it seems we are stuck with this
particular string format.

BigQueryWriter also transforms a timestamp field to this string format before sending it on to BigQuery,
However, it will accept a float value, so you can just send a unix timestamp and it will get handled
properly.


Timezone Issues
---------------

Python datetimes have an issue with timezones.  The default usage for datetime creates a timezone naive
value with no specific timezone.  Unfortunatly, in some circumstances this value can end up being interprested
as a value in the local timezone, which can break things if you can converting to and from UTC timestamps

This library creates only timezone aware datetimes to guard against this.  This is for your protection :-)
If you end up  trying to convert a timezone naive value you will get something like this error

   TypeError: can't subtract offset-naive and offset-aware datetimes

Create a timezone aware datetime with something like this:

  import pytz
  now = datetime.now(tz=pytz.UTC)


NOTE about udatetime vs datetime
--------------------------------

udatetime is much faster than the native python datetime, particularly for parsing,
and it handles timezones better.  However it does not pickle in python 2.7, so it cannot be
passed around in an apache beam PCollection.  Therefore conversions for both types are provided.

When in doubt, use the datetime methods.  If you know that you will only be using it within a function,
then you can use udatetime.

The best thing to do is to keep timestamps as floats all the time except when you explicity need a
datetime.  And if you need to write one out to json, use a float or a RFC3339 string

"""

T = beam.typehints.TypeVariable("T")


def datetimeFromTimestamp(ts):
    """Convert a unix timestamp to a datetime"""
    return datetime.fromtimestamp(ts, tz=pytz.UTC)


def timestampFromDatetime(dt):
    """Convert a datetime to a unix timestamp"""
    return (dt - EPOCH).total_seconds()


def datetimeFromBeamBQStr(s):
    """convert a Beam/BigQuery specific string representation of a timestamp to a datetime.

    The string must match BEAM_BQ_TIMESTAMP_FORMAT
    """
    # return udatetime.parse(rfc3339strFromBeamBQStr(s))

    # have to force the timezone because strptime parses it, but then ignores it
    return pytz.UTC.localize(datetime.strptime(s, BEAM_BQ_TIMESTAMP_FORMAT))


def beambqstrFromDatetime(dt):
    return dt.strftime(BEAM_BQ_TIMESTAMP_FORMAT)


def rfc3339strFromBeamBQStr(s):
    """Convert a string formatted BEAM_BQ_TIMESTAMP_FORMAT to RFC3339_TIMESTAMP_FORMAT"""
    return s[:-4].replace(" ", "T") + "+00:00"


def udatetimeFromTimestamp(ts):
    """Convert a unix timestamp to a udatetime"""
    return udatetime.utcfromtimestamp(ts)


def timestampFromUdatetime(udt):
    """Convert a unix timestamp to a udatetime"""
    return (udt - EPOCH).total_seconds()


def udatetimeFromBeamBQStr(s):
    """Convert a string formatted BEAM_BQ_TIMESTAMP_FORMAT to a udatetime"""
    return udatetime.from_string(rfc3339strFromBeamBQStr(s))


def timestampFromBeamBQStr(s):
    """convert a string representation of a timestamp to a unix timestamp"""
    return timestampFromDatetime(udatetimeFromBeamBQStr(s))


def beambqstrFromTimestamp(ts):
    return datetime.utcfromtimestamp(ts).strftime(BEAM_BQ_TIMESTAMP_FORMAT)


def rfc3339strFromTimestamp(ts):
    return udatetime.to_string(udatetime.utcfromtimestamp(ts))


def timestampFromRfc3339str(s):
    return timestampFromUdatetime(udatetime.from_string(s))


class ParseBeamBQStrTimestampDoFn(beam.DoFn):
    """Convert timestamp fields from Beam/BigQuery formatted string to timestamp

    Assumes that the elements in the pcoll are dicts

    fields: a list of dict keys that should be converted from string to unix timestamp

    Note that this transform modifies the dict in place.  If some previous process has a reference to
    this dict, then unexpected things can happen.   This can happen in beam even across
    PTransforms because they are often run in the same process in the same node, so the
    PCollection elements do not get pickled in between.  Whether or not this happens
    depends on timing, so it may happen intermittently

    If you are having problems with timestamp fields occasionally showing up with string instead of
    float or the other way around, use SafeParseBeamBQStrTimestamp instead. This is slower, but safer.

    """

    def __init__(self, fields=["timestamp"]):
        self._fields = fields
        if isinstance(self._fields, six.string_types):
            self._fields = [self._fields]
        super(ParseBeamBQStrTimestampDoFn, self).__init__()

    def process(self, element):
        for f in self._fields:
            v = element.get(f)
            if v is not None:
                element[f] = timestampFromBeamBQStr(v)
        yield element


class SafeParseBeamBQStrTimestampDoFn(beam.DoFn):
    """Convert timestamp fields from Beam/BigQuery formatted string to timestamp

    Assumes that the elements in the pcoll are dicts

    fields: a list of dict keys that should be converted from string to unix timestamp

    See note in ParseBeamBQStrTimestamp about why we need this class.  If you are using this
    immediately after a read from BigQuery, then you can probably use ParseBeamBQStrTimestamp
    instead.

    Note that if your dict has nested iterables like a list, then this is not really safe either and
    you will really want something that does a deepcopy
    """

    def __init__(self, fields=["timestamp"]):
        self.fields = fields
        if isinstance(self.fields, six.string_types):
            self.fields = [self.fields]
        super(SafeParseBeamBQStrTimestampDoFn, self).__init__()

    def process(self, element):
        new_element = element.copy()
        for f in self.fields:
            v = new_element.get(f)
            if v is not None:
                new_element[f] = timestampFromBeamBQStr(v)
        yield new_element


class TimestampedValueDoFn(beam.DoFn):
    """Use this to extract a timestamp from a message.  process() expects a dict with the
    specified field containing a unix timestamp"""

    def __init__(self, field="timestamp"):
        self.timestamp_field = field

    def process(self, msg):
        # Wrap and emit the current entry and new timestamp in a
        # TimestampedValue.
        yield TimestampedValue(msg, msg[self.timestamp_field])


def as_timestamp(d):
    """
    Attempt to convert the passed parameter to a datetime.  Note that for strings this
    uses python-dateutil, which is flexible, but SLOW.  So do not use this methig if you
    are going to be parsing a billion dates.  Use the high performance methods in timestamp.py

    return: float   seconds since epoch (unix timestamp)
    """
    if d is None:
        return None
    elif isinstance(d, datetime):
        return timestampFromDatetime(pytz.UTC.localize(d) if d.tzinfo is None else d)
        # return timestampFromDatetime(pytz.UTC.localize(d))
    elif isinstance(d, date):
        return timestampFromDatetime(
            pytz.UTC.localize(datetime.combine(d, datetime.min.time()))
        )
    elif isinstance(d, (float, int, Timestamp)):
        return float(d)
    elif isinstance(d, six.string_types):
        return as_timestamp(dateutil_parse(d))
    else:
        raise ValueError(
            'Unsupported data type. Unable to convert value "%s" to to a timestamp' % d
        )
