import argparse
import datetime
import re

TABLE_REGEX = r"[\w\-_]+\.[\w\-_]+\.[\w\-_]+"


def valid_daterange(s: str) -> str:
    # expects to have YYYY-MM-DD,YYYY-MM-DD
    s1, s2 = s.split(',')
    assert valid_date(s1) <= valid_date(s2)
    return s


def valid_date(s: str) -> datetime.date:
    try:
        return datetime.datetime.strptime(s, "%Y-%m-%d").date()
    except ValueError:
        raise argparse.ArgumentTypeError(f"not a valid date: {s!r}")


def valid_table_reference(s: str) -> str:
    matched = re.fullmatch(TABLE_REGEX, s)
    if matched is None:
        raise argparse.ArgumentTypeError(
            f"not a valid table pattern: {s!r}. Format allowed <{TABLE_REGEX}>"
        )
    return s
