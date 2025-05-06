import pytest
from datetime import date
from pipe_segment.cli.commands.validator import (
    valid_date,
    valid_daterange,
    valid_table_fullpath,
    valid_table_shortpath,
    valid_frequency
)


class TestValidators:

    @pytest.mark.parametrize(
        "entry,expected",
        [
            ("2020-01-01", date(2020, 1, 1)),
            ("2024-02-29", date(2024, 2, 29)),
            pytest.param("test", "test", marks=pytest.mark.xfail),
        ]
    )
    def test_valid_date(self, entry, expected):
        assert expected == valid_date(entry)

    @pytest.mark.parametrize(
        "entry,expected",
        [
            ("2020-01-01,2020-01-01", "2020-01-01,2020-01-01"),
            pytest.param("2024-02-29,2024-02-28", "test", marks=pytest.mark.xfail),
        ]
    )
    def test_valid_daterange(self, entry, expected):
        assert expected == valid_daterange(entry)

    @pytest.mark.parametrize(
        "table,expected",
        [
            ("a.b.c", "a.b.c"),
            ("a:b.c", "a:b.c"),
            ("bq://a:b.c", "bq://a:b.c"),
            ("a-x.b-y.c-z", "a-x.b-y.c-z"),
            pytest.param("b.c", "b.c", marks=pytest.mark.xfail),
            pytest.param("bq://b.c", "bq://b.c", marks=pytest.mark.xfail),
            pytest.param("a-b.c", "a-b.c", marks=pytest.mark.xfail),
            pytest.param("test", "test", marks=pytest.mark.xfail),
        ]
    )
    def test_table_valid_fullpath(self, table, expected):
        assert expected == valid_table_fullpath(table)

    @pytest.mark.parametrize(
        "table,expected",
        [
            ("b.c", "b.c"),
            ("bq://b.c", "bq://b.c"),
            ("b-y.c-z", "b-y.c-z"),
            pytest.param("bq://a:b.c", "bq://a:b.c", marks=pytest.mark.xfail),
            pytest.param("a-b.c", "a-b.c", marks=pytest.mark.xfail),
            pytest.param("test", "test", marks=pytest.mark.xfail),
        ]
    )
    def test_table_valid_shortpath(self, table, expected):
        assert expected == valid_table_shortpath(table)

    @pytest.mark.parametrize(
        "value,expected",
        [
            (0.1, 0.1),
            pytest.param(1.2, 1.2, marks=pytest.mark.xfail),
            pytest.param(-1.2, -1.2, marks=pytest.mark.xfail),
        ]
    )
    def test_value_valid_shortpath(self, value, expected):
        assert expected == valid_frequency(value)
