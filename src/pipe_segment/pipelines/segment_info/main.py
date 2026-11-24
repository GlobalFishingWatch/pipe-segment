"""Entry point of the segment-info pipeline."""

import logging

from typing import Any, Callable

from gfw.common.bigquery.helper import BigQueryHelper
from pipe_segment.pipelines.segment_info.config import SegmentInfoConfig
from pipe_segment.pipelines.segment_info.queries import SegmentInfoQuery
from pipe_segment.pipelines.segment_info.table_config import (
    SegmentInfoTableConfig,
    SegmentInfoTableDescription,
)
from pipe_segment.version import __version__


logger = logging.getLogger(__name__)


def run(
    config: SegmentInfoConfig,
    bq_client_factory: Callable | None = None,
    **kwargs: Any,
) -> None:
    """Runs the segment-info pipeline.

    Args:
        config:
            The resolved configuration.

        bq_client_factory:
            Builds the BigQuery client. Defaults to gfw-common's, mocked if
            config.mock_bq_clients is set.

        **kwargs:
            Extra arguments of the CLI, which this pipeline does not use.
    """
    if bq_client_factory is None:
        bq_client_factory = BigQueryHelper.get_client_factory(mocked=config.mock_bq_clients)

    bq = BigQueryHelper(
        client_factory=bq_client_factory,
        dry_run=config.dry_run,
        project=config.project,
    )

    query = SegmentInfoQuery(
        segment_identity_daily=config.bq_in_segment_identity_daily,
        segment_vessel_daily=config.bq_in_segment_vessel_daily,
        most_common_min_freq=config.most_common_min_freq,
    ).with_env(config.jinja_env)

    table_config = SegmentInfoTableConfig(
        table_id=config.bq_out_segment_info,
        description=SegmentInfoTableDescription(
            version=__version__,
            relevant_params={
                "bq_in_segment_identity_daily": config.bq_in_segment_identity_daily,
                "bq_in_segment_vessel_daily": config.bq_in_segment_vessel_daily,
                "most_common_min_freq": config.most_common_min_freq,
            },
        ),
    )

    logger.info(f"Writing {table_config.table_id}...")
    bq.run_query(
        query_str=query.render(),
        destination=table_config.table_id,
        labels=config.labels,
        write_disposition="WRITE_TRUNCATE",
        clustering_fields=table_config.clustering_fields,
        schema=table_config.schema,
        description=table_config.description.render(),
        allow_table_recreation=True,
    )
    logger.info("Done.")
