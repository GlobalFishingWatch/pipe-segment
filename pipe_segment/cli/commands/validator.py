import argparse
import datetime
import re

TABLE_REGEX = r"[\w\-_]+\.[\w\-_]+\.[\w\-_]+"


def valid_frequency(f: float) -> float:
    """Validates the frequency, raises error if not."""
    if (f < 0 or f > 1):
        return ValueError(f"Value should be between 0 and 1, value: {f}")
    else:
        return f


def valid_daterange(s: str) -> str:
    """Validates to be a date range following the timeline precedence. Raises error if not."""
    # expects to have YYYY-MM-DD,YYYY-MM-DD
    s1, s2 = s.split(',')
    assert valid_date(s1) <= valid_date(s2)
    return s


def valid_date(s: str) -> datetime.date:
    """Validates to be a date, raises error if not."""
    try:
        return datetime.datetime.strptime(s, "%Y-%m-%d").date()
    except ValueError:
        raise argparse.ArgumentTypeError(f"not a valid date: {s!r}")


def valid_table_reference(s: str) -> str:
    """Validates to be a full table id, raises error if not."""
    matched = re.fullmatch(TABLE_REGEX, s)
    if matched is None:
        raise argparse.ArgumentTypeError(
            f"not a valid table pattern: {s!r}. Format allowed <{TABLE_REGEX}>"
        )
    return s


def valid_positive_int(i: int) -> int:
    """Validates to be a positive int, raises error if not."""
    if not isinstance(i, int):
        raise ValueError("Expecting an int.")
    if i < 0:
        raise ValueError("Expecting a positive int.")
    return i
