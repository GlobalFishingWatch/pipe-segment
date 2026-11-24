"""Entry point of the segment-vessel-daily pipeline."""

import logging

from typing import Any, Callable

from gfw.common.bigquery.helper import BigQueryHelper
from pipe_segment.pipelines.segment_vessel_daily.config import SegmentVesselDailyConfig
from pipe_segment.pipelines.segment_vessel_daily.queries import SegmentVesselDailyQuery
from pipe_segment.pipelines.segment_vessel_daily.table_config import (
    SegmentVesselDailyTableConfig,
    SegmentVesselDailyTableDescription,
)
from pipe_segment.tools import list_of_days
from pipe_segment.version import __version__


logger = logging.getLogger(__name__)


def run(
    config: SegmentVesselDailyConfig,
    bq_client_factory: Callable | None = None,
    **kwargs: Any,
) -> None:
    """Runs the segment-vessel-daily pipeline.

    Creates the output table if needed, deletes the days of the range and appends them, one
    query per day. With ``dry_run`` it only runs the queries of the days, as dry run jobs.

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

    table_config = SegmentVesselDailyTableConfig(
        table_id=config.bq_out_segment_vessel_daily,
        description=SegmentVesselDailyTableDescription(
            version=__version__,
            relevant_params={
                "bq_in_segment_identity_daily": config.bq_in_segment_identity_daily,
                "start_date": config.start_date,
                "end_date": config.end_date,
                "window_days": config.window_days,
                "single_ident_min_freq": config.single_ident_min_freq,
                "most_common_min_freq": config.most_common_min_freq,
                "spoofing_threshold": config.spoofing_threshold,
            },
        ),
    )

    if not config.dry_run:
        bq.create_table(**table_config.to_bigquery_params(), labels=config.labels, exists_ok=True)
        logger.info(f"Deleting the days [{config.start_date}, {config.end_date})...")
        bq.run_query(
            table_config.delete_query(config.start_date, config.end_date),
            labels=config.labels,
        )

    for day in list_of_days(config.start_date, config.end_date):
        query = SegmentVesselDailyQuery(
            date=day,
            segment_identity=config.bq_in_segment_identity_daily,
            window_days=config.window_days,
            single_ident_min_freq=config.single_ident_min_freq,
            most_common_min_freq=config.most_common_min_freq,
            spoofing_threshold=config.spoofing_threshold,
        ).with_env(config.jinja_env)

        logger.info(f"Writing {table_config.table_id} for {day}...")
        bq.run_query(
            query_str=query.render(),
            destination=table_config.table_id,
            labels=config.labels,
            write_disposition="WRITE_APPEND",
            partition_field=table_config.partition_field,
            partition_type=table_config.partition_type,
            clustering_fields=table_config.clustering_fields,
            schema=table_config.schema,
            description=table_config.description.render(),
        )

    logger.info("Done.")
