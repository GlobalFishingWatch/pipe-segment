from datetime import date
from types import SimpleNamespace

import pytest

from google.api_core.exceptions import NotFound
from google.cloud import bigquery

from pipe_segment.pipelines.messages_segmented.hooks import (
    verify_messages_exist_hook,
    verify_prior_segments_hook,
)


DAY = date(2024, 4, 11)
SEGMENTS = "project.dataset.segments"
FRAGMENTS = "project.dataset.fragments"
LABELS = {"a": "b"}


@pytest.fixture
def pipe():
    return SimpleNamespace(cloud_options=SimpleNamespace(project="the-project"))


@pytest.fixture
def client(mocker):
    """BigQuery client mock. Set `scalars` to the values that its queries return, in order."""
    client = mocker.Mock()
    client.scalars = []
    client.query_and_wait.side_effect = lambda *args, **kwargs: iter([[client.scalars.pop(0)]])
    client.get_table.side_effect = lambda table_id: bigquery.Table(table_id)
    return client


@pytest.fixture
def bq_client_factory(mocker, client):
    return mocker.Mock(return_value=client)


def queries(client):
    return [call.args[0] for call in client.query_and_wait.call_args_list]


def messages_hook(bq_client_factory, sources=("project.dataset.messages",)):
    return verify_messages_exist_hook(
        list(sources),
        date(2026, 11, 4),
        date(2026, 11, 10),
        labels=LABELS,
        bq_client_factory=bq_client_factory,
    )


def prior_segments_hook(bq_client_factory):
    return verify_prior_segments_hook(
        SEGMENTS, FRAGMENTS, DAY, labels=LABELS, bq_client_factory=bq_client_factory
    )


def test_messages_hook_passes_when_every_day_has_messages(pipe, client, bq_client_factory):
    client.scalars = [7]

    messages_hook(bq_client_factory)(pipe)

    assert "COUNT(DISTINCT day)" in queries(client)[0]
    assert "BETWEEN '2026-11-04'" in queries(client)[0]
    assert "AND '2026-11-10'" in queries(client)[0]


def test_messages_hook_fails_when_a_day_has_no_messages(pipe, client, bq_client_factory):
    client.scalars = [6]

    with pytest.raises(ValueError, match="Only 6 of 7 days between 2026-11-04 and 2026-11-10"):
        messages_hook(bq_client_factory)(pipe)


def test_messages_hook_counts_the_days_of_all_sources_together(pipe, client, bq_client_factory):
    client.scalars = [7]

    messages_hook(bq_client_factory, ("project.dataset.a", "project.dataset.b"))(pipe)

    query = queries(client)[0]
    assert "project.dataset.a" in query
    assert "project.dataset.b" in query
    assert "UNION ALL" in query


def test_messages_hook_parses_the_suffix_of_sharded_sources(pipe, client, bq_client_factory):
    client.scalars = [7]

    def get_table(table_id):
        if not table_id.endswith("*"):
            raise NotFound("not found")
        return bigquery.Table(table_id.rstrip("*"))

    client.get_table.side_effect = get_table

    messages_hook(bq_client_factory)(pipe)

    assert "PARSE_DATE('%Y%m%d', _TABLE_SUFFIX)" in queries(client)[0]


def test_prior_segments_hook_passes_when_the_day_has_segments(pipe, client, bq_client_factory):
    client.scalars = [10]

    prior_segments_hook(bq_client_factory)(pipe)

    assert "DATE(timestamp) = '2024-04-11'" in queries(client)[0]


def test_prior_segments_hook_does_not_check_the_history_when_the_day_has_segments(
    pipe, client, bq_client_factory
):
    client.scalars = [10]

    prior_segments_hook(bq_client_factory)(pipe)

    assert len(queries(client)) == 1


def test_prior_segments_hook_passes_on_the_first_run_of_the_history(
    pipe, client, bq_client_factory
):
    client.scalars = [0, False]

    prior_segments_hook(bq_client_factory)(pipe)

    assert "`project.dataset.fragments`" in queries(client)[1]
    assert "DATE(timestamp) <= '2024-04-11'" in queries(client)[1]


def test_prior_segments_hook_fails_when_the_day_has_no_segments_but_there_is_history(
    pipe, client, bq_client_factory
):
    client.scalars = [0, True]

    with pytest.raises(ValueError, match="has no segments on 2024-04-11"):
        prior_segments_hook(bq_client_factory)(pipe)


def test_hooks_use_the_project_of_the_pipeline_and_the_labels(pipe, client, bq_client_factory):
    client.scalars = [10]

    prior_segments_hook(bq_client_factory)(pipe)

    bq_client_factory.assert_called_once_with(project="the-project")
    job_config = client.query_and_wait.call_args.kwargs["job_config"]
    assert job_config.labels == LABELS
