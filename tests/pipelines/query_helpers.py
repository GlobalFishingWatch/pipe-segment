"""Helpers of the tests of the pipelines that run their queries on BigQuery."""

from types import ModuleType
from typing import Any, Optional
from unittest.mock import MagicMock

from google.cloud.bigquery import QueryJobConfig

from gfw.common.bigquery.helper import BigQueryHelper


def run_with_client(
    main: ModuleType,
    config: Any,
    clustering_fields: Optional[list[str]] = None,
) -> MagicMock:
    """Runs the pipeline and returns the mocked BigQuery client it used.

    Args:
        main:
            Module of the pipeline, with its ``run``.

        config:
            Configuration of the pipeline.

        clustering_fields:
            If given, the output table already has that layout (partitioned by nothing and
            clustered by these fields), so it is not recreated.

    Returns:
        The mocked client.
    """
    mocked_factory = BigQueryHelper.get_client_factory(mocked=True)
    clients = []

    def client_factory(**kwargs: Any) -> MagicMock:
        client = mocked_factory(**kwargs)
        client.default_query_job_config = kwargs["default_query_job_config"]
        if clustering_fields is not None:
            client.get_table.return_value.time_partitioning = None
            client.get_table.return_value.clustering_fields = clustering_fields
        clients.append(client)
        return client

    main.run(config, bq_client_factory=client_factory)
    return clients[0]


def written_job_config(client: MagicMock) -> QueryJobConfig:
    """Returns the job config of the query that writes to the output table."""
    job_configs = [call.kwargs["job_config"] for call in client.query.call_args_list]
    return [job_config for job_config in job_configs if job_config.destination is not None][-1]
