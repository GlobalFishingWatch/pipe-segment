import pytest
from datetime import date
from pipe_segment.cli.commands.validator import (
    valid_date,
    valid_daterange,
    valid_table_reference,
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
            ("a-x.b-y.c-z", "a-x.b-y.c-z"),
            pytest.param("b.c", "b.c", marks=pytest.mark.xfail),
            pytest.param("a-b.c", "a-b.c", marks=pytest.mark.xfail),
            pytest.param("test", "test", marks=pytest.mark.xfail),
        ]
    )
    def test_valid_table_reference(self, table, expected):
        assert expected == valid_table_reference(table)
