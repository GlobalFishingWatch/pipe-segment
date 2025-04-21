import argparse
import datetime
import re

FULL_TABLE_PATH = r"(bq://)?[\w\-_]+[:\.][\w\-_]+\.[\w\-_]+"
SHORT_TABLE_PATH = r"(bq://)?[\w\-_]+\.[\w\-_]+"


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


def valid_table(s: str, matched: str, pattern: str) -> str:
    if matched is None:
        raise argparse.ArgumentTypeError(
            f"not a valid table pattern: {s!r}. Format allowed <{pattern}>"
        )
    return s


def valid_table_shortpath(s: str) -> str:
    matched = re.fullmatch(SHORT_TABLE_PATH, s)
    return valid_table(s, matched, SHORT_TABLE_PATH)


def valid_table_fullpath(s: str) -> str:
    matched = re.fullmatch(FULL_TABLE_PATH, s)
    return valid_table(s, matched, FULL_TABLE_PATH)
