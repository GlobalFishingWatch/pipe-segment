from dataclasses import dataclass, field
from datetime import date

import pytest

from pipe_segment.transform.read_prior_segments import ReadPriorSegments


@dataclass
class BigQueryHelperMock:
    scalar: int = 1
    labels: dict = field(default_factory=dict)
    queries: list = field(default_factory=list)

    def fetch_scalar(self, query):
        self.queries.append(query)
        return self.scalar


def make_transform(bq_helper, ssvid_filter_query=None):
    return ReadPriorSegments(
        bq_helper,
        "project.dataset.segments",
        "project.dataset.fragments",
        date(2024, 4, 11),
        ssvid_filter_query=ssvid_filter_query,
    )


def test_build_query_reads_the_day_from_both_tables():
    query = make_transform(BigQueryHelperMock()).build_query()

    assert "FROM `project.dataset.segments` AS s" in query
    assert "JOIN `project.dataset.fragments` AS f" in query
    assert "DATE(s.timestamp) = '2024-04-11'" in query
    assert "DATE(f.timestamp) = '2024-04-11'" in query
    assert "s.ssvid IN" not in query


def test_build_query_filters_ssvids():
    query = make_transform(BigQueryHelperMock(), "SELECT ssvid FROM t").build_query()

    assert "AND s.ssvid IN (SELECT ssvid FROM t)" in query


def test_verify_passes_when_the_day_has_segments():
    bq_helper = BigQueryHelperMock(scalar=10)

    make_transform(bq_helper).verify()

    assert "DATE(timestamp) = '2024-04-11'" in bq_helper.queries[0]


def test_verify_fails_when_the_day_has_no_segments():
    with pytest.raises(ValueError, match="has no segments on 2024-04-11"):
        make_transform(BigQueryHelperMock(scalar=0)).verify()
