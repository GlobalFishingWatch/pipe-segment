import argparse
import datetime
import re


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


def valid_table(s: str, pattern: str) -> str:
    if pattern is None:
        raise argparse.ArgumentTypeError(
            f"not a valid table pattern: {s!r}"
        )
    return s


def valid_table_shortpath(s: str) -> str:
    matched_shortpath = re.fullmatch(r"(bq://)?[\w\-_]+\.[\w\-_]+", s)
    return valid_table(s, matched_shortpath)


def valid_table_fullpath(s: str) -> str:
    matched_fullpath = re.fullmatch(r"(bq://)?[\w\-_]+[:\.][\w\-_]+\.[\w\-_]+", s)
    return valid_table(s, matched_fullpath)
