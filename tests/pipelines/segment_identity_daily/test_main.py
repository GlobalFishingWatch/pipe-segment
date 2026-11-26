"""Tests for the segment-identity-daily pipeline, run on mocked BigQuery."""

from collections.abc import Callable
from datetime import date
from typing import Any

import apache_beam as beam
import pytest

from apache_beam.pvalue import PCollection
from apache_beam.testing.util import assert_that
from pytest_mock import MockerFixture

from gfw.common.beam.transforms.bigquery import FakeReadFromBigQuery
from gfw.common.bigquery.helper import BigQueryHelper
from pipe_segment.pipelines.segment_identity_daily import main
from pipe_segment.pipelines.segment_identity_daily.config import SegmentIdentityDailyConfig
from pipe_segment.pipelines.segment_identity_daily.table_config import (
    SegmentIdentityDailyTableConfig,
)


# Beam's local runner, named explicitly: DirectRunner would fall back to other runners for
# pipelines Prism doesn't support.
RUNNER = "PrismRunner"

TIMESTAMP = 1500508800.0  # 2017-07-20 00:00:00 UTC.


def make_config(**overrides: Any) -> SegmentIdentityDailyConfig:
    """Returns the pipeline's config for the test dates, with the fields overridden."""
    fields = {
        "project": "test-project",
        "bq_in_segments": "project.dataset.segments",
        "bq_in_fragments": "project.dataset.fragments",
        "bq_out_segment_identity_daily": "project.dataset.segment_identity_daily",
        "start_date": date(2024, 1, 1),
        "end_date": date(2024, 1, 3),
        "labels": {"environment": "test"},
        "mock_bq_clients": True,
    }
    fields.update(overrides)
    return SegmentIdentityDailyConfig(**fields)


def segment_row() -> dict[str, Any]:
    """Returns a row shaped like the segments query's output."""
    return {
        "seg_id": "338013000-2017-07-16T19:15:55.000000Z-1",
        "frag_id": "338013000-2017-07-20T00:27:29.000000Z-1",
        "ssvid": "338013000",
        "timestamp": TIMESTAMP,
        "first_msg_timestamp": TIMESTAMP + 60,
        "last_msg_timestamp": TIMESTAMP + 120,
        "daily_msg_count": 10,
        "daily_identities": [
            {
                "count": 3,
                "shipname": "Friesland",
                "callsign": "WDE6789",
                "imo": None,
                "length": None,
                "width": None,
            },
        ],
    }


class AssertWritten(beam.PTransform):
    """Stands in for WriteToBigQuery: checks the rows to be written, inside the pipeline."""

    def __init__(self, check: Callable[[list[dict[str, Any]]], None], **kwargs: Any) -> None:
        """Stores the check to run on the rows written."""
        super().__init__()
        self._check = check

    def expand(self, pcoll: PCollection) -> PCollection:
        """Checks the rows, then passes them on."""
        assert_that(pcoll, self._check)
        return pcoll


def check_one_summary(rows: list[dict[str, Any]]) -> None:
    """Checks that the one segment is summarized, with the fields of the table."""
    schema = SegmentIdentityDailyTableConfig(table_id="project.dataset.table").schema
    (row,) = rows
    assert set(row) == {field["name"] for field in schema}
    assert row["summary_timestamp"] == TIMESTAMP
    assert row["pos_count"] == 10
    assert row["ident_count"] == 3
    assert row["msg_count"] == 13
    assert row["n_shipname"] == [{"value": "FRIESLAND", "count": 3}]


def test_run() -> None:
    """Runs the pipeline on mocked BigQuery, then checks the query, the rows and the deletes."""
    reads: list[dict[str, Any]] = []
    writes: list[dict[str, Any]] = []
    bq_clients: list[Any] = []

    def read_factory(**kwargs: Any) -> FakeReadFromBigQuery:
        reads.append(kwargs)
        return FakeReadFromBigQuery(elements=[segment_row()], **kwargs)

    def write_factory(**kwargs: Any) -> AssertWritten:
        writes.append(kwargs)
        return AssertWritten(check_one_summary, **kwargs)

    def bq_client_factory(**kwargs: Any) -> Any:
        bq_clients.append(BigQueryHelper.get_client_factory(mocked=True)(**kwargs))
        return bq_clients[-1]

    main.run(
        make_config(),
        read_from_bigquery_factory=read_factory,
        write_to_bigquery_factory=write_factory,
        bq_client_factory=bq_client_factory,
        runner=RUNNER,
    )

    assert len(reads) == 1
    assert "`project.dataset.segments`" in reads[0]["query"]
    assert "`project.dataset.fragments`" in reads[0]["query"]
    assert reads[0]["bigquery_job_labels"] == {"environment": "test"}
    assert writes[0]["table"] == "project.dataset.segment_identity_daily"
    assert writes[0]["write_disposition"] == "WRITE_APPEND"
    assert writes[0]["create_disposition"] == "CREATE_NEVER"

    queries = [call for client in bq_clients for call in client.query.call_args_list]
    (delete,) = [call for call in queries if call.args[0].startswith("DELETE")]
    assert "< '2024-01-03'" in delete.args[0]
    assert delete.kwargs["job_config"].labels == {"environment": "test"}


@pytest.mark.parametrize("wait_for_job", [True, False])
def test_run_passes_wait_for_job_to_the_pipeline(
    mocker: MockerFixture, wait_for_job: bool
) -> None:
    """Checks that the pipeline waits as the config says, with no argument to run."""
    pipeline_class = mocker.patch.object(main, "Pipeline")

    main.run(make_config(wait_for_job=wait_for_job))

    assert pipeline_class.call_args.kwargs["wait_for_job"] is wait_for_job
    pipeline_class.return_value.run.assert_called_once_with()
