from dataclasses import dataclass, field
from datetime import date

import pytest

from google.cloud import bigquery

from pipe_segment.transform.read_messages import ReadMessages


@dataclass
class BigQueryHelperMock:
    days: int = 7
    sharded: bool = False
    labels: dict = field(default_factory=dict)
    queries: list = field(default_factory=list)

    def fetch_table(self, table_ref):
        if self.sharded and not table_ref.endswith("*"):
            return None
        return bigquery.Table(table_ref.rstrip("*"))

    def fetch_scalar(self, query):
        self.queries.append(query)
        return self.days


def make_transform(bq_helper, sources=("project.dataset.messages",)):
    return ReadMessages(bq_helper, list(sources), date(2026, 11, 4), date(2026, 11, 10))


def test_verify_passes_when_every_day_has_messages():
    bq_helper = BigQueryHelperMock(days=7)

    make_transform(bq_helper).verify()

    assert "COUNT(DISTINCT day)" in bq_helper.queries[0]
    assert "BETWEEN '2026-11-04'" in bq_helper.queries[0]
    assert "AND '2026-11-10'" in bq_helper.queries[0]


def test_verify_fails_when_a_day_has_no_messages():
    with pytest.raises(ValueError, match="Only 6 of 7 days between 2026-11-04 and 2026-11-10"):
        make_transform(BigQueryHelperMock(days=6)).verify()


def test_verify_counts_the_days_of_all_sources_together():
    bq_helper = BigQueryHelperMock(days=7)

    make_transform(bq_helper, ("project.dataset.a", "project.dataset.b")).verify()

    query = bq_helper.queries[0]
    assert "project.dataset.a" in query
    assert "project.dataset.b" in query
    assert "UNION ALL" in query


def test_verify_parses_the_suffix_of_sharded_sources():
    bq_helper = BigQueryHelperMock(days=7, sharded=True)

    make_transform(bq_helper).verify()

    assert "PARSE_DATE('%Y%m%d', _TABLE_SUFFIX)" in bq_helper.queries[0]
