"""Pre-hooks of the segment pipeline: checks that must pass before it changes anything.

They are plain callables that take the pipeline, with the shape of the hooks of
gfw-common's Pipeline, so they can be moved there unchanged. A check runs once when the
pipeline is built, so it is not part of any transform: a transform should only carry the
plain values it needs, never a BigQuery client.
"""

import logging

from datetime import date
from typing import Any, Callable, Optional

from google.cloud import bigquery

from gfw.common.bigquery.helper import BigQueryHelper
from pipe_segment.pipelines.messages_segmented.queries import MessageDaysQuery


logger = logging.getLogger(__name__)


def verify_messages_exist_hook(
    sources: list[str],
    start_date: date,
    end_date: date,
    *,
    labels: dict[str, str],
    bq_client_factory: Optional[Callable[..., bigquery.Client]] = None,
) -> Callable[[Any], None]:
    """Returns a hook function that checks that every day to process has messages.

    The days are counted over all the sources together.

    Args:
        sources:
            Tables with the messages.

        start_date:
            First day to process, included.

        end_date:
            Last day to process, included.

        labels:
            Labels to set on the query job.

        bq_client_factory:
            Factory for the BigQuery client. Defaults to a real client. Pass a mocked one
            to run without BigQuery.

    Returns:
        A callable hook that accepts the pipeline and raises :class:`ValueError`
        if a day between the dates has no messages in the sources.
    """

    def _hook(p: Any) -> None:
        bq_helper = _bq_helper(p, bq_client_factory)
        n_days = (end_date - start_date).days + 1
        query = MessageDaysQuery(sources, start_date, end_date).render()
        days_with_messages = fetch_scalar(bq_helper, labels, query)
        if days_with_messages < n_days:
            raise ValueError(
                f"Only {days_with_messages} of {n_days} days between {start_date} and "
                f"{end_date} have messages in {sources}. "
                "An incremental run needs the messages of every day of the date range."
            )

    return _hook


def verify_prior_segments_hook(
    segments_table: str,
    fragments_table: str,
    day: date,
    *,
    labels: dict[str, str],
    bq_client_factory: Optional[Callable[..., bigquery.Client]] = None,
) -> Callable[[Any], None]:
    """Returns a hook function that checks that the state before the date range can be read.

    Starting from an empty state would create new segment ids for every open segment,
    so the run must stop if the segments table has no rows for the day but there is
    earlier history. If there are no fragments up to that day either, this is the first
    run in the history and an empty state is correct.

    Args:
        segments_table:
            Table with the segments-days.

        fragments_table:
            Table with the fragments.

        day:
            Day to read the state from. It is the day before the first day to process.

        labels:
            Labels to set on the query jobs.

        bq_client_factory:
            Factory for the BigQuery client. Defaults to a real client. Pass a mocked one
            to run without BigQuery.

    Returns:
        A callable hook that accepts the pipeline and raises :class:`ValueError`
        if the day has no segments but there are fragments up to it.
    """

    def _hook(p: Any) -> None:
        bq_helper = _bq_helper(p, bq_client_factory)
        count = fetch_scalar(
            bq_helper,
            labels,
            f"SELECT COUNT(*) FROM `{segments_table}` WHERE DATE(timestamp) = '{day}'",
        )
        if count > 0:
            return

        has_history = fetch_scalar(
            bq_helper,
            labels,
            f"""
            SELECT EXISTS (
              SELECT
                timestamp
              FROM `{fragments_table}`
              WHERE DATE(timestamp) <= '{day}'
            )
            """,
        )
        if not has_history:
            logger.info(f"No fragments up to {day}: starting from an empty state.")
            return

        raise ValueError(
            f"Table {segments_table} has no segments on {day}, but "
            f"{fragments_table} has fragments up to that day. "
            "An incremental run needs the segments of the day before the date range. "
            "Run the previous days first, or run without --incremental to rebuild "
            "from the whole fragment history."
        )

    return _hook


def _bq_helper(
    p: Any,
    bq_client_factory: Optional[Callable[..., bigquery.Client]],
) -> BigQueryHelper:
    """Builds the helper that a hook uses, in the project the pipeline runs in.

    Args:
        p:
            The pipeline that runs the hook. It has the project in its cloud options.

        bq_client_factory:
            Factory for the BigQuery client. Defaults to a real client.

    Returns:
        The helper.
    """
    if bq_client_factory is None:
        bq_client_factory = bigquery.Client

    return BigQueryHelper(client_factory=bq_client_factory, project=p.cloud_options.project)


def fetch_scalar(bq_helper: BigQueryHelper, labels: dict[str, str], query: str) -> Any:
    """Runs a query that returns a single row with a single column, and returns its value.

    Args:
        bq_helper:
            Helper to run the query.

        labels:
            Labels to set on the query job.

        query:
            The query.

    Returns:
        The value.
    """
    return next(iter(bq_helper.run_query(query, labels=labels)))[0]
