"""Entry point of the segment-identity-daily pipeline."""

from typing import Any, Callable

import apache_beam as beam

from gfw.common.beam.pipeline.base import Pipeline
from gfw.common.beam.pipeline.dag import LinearDag
from gfw.common.beam.pipeline.hooks import (
    create_table_hook,
    delete_events_hook,
    update_table_hook,
)
from gfw.common.beam.transforms import ReadFromBigQuery, WriteToBigQueryWrapper
from gfw.common.bigquery.helper import BigQueryHelper
from pipe_segment.pipelines.segment_identity_daily.config import SegmentIdentityDailyConfig
from pipe_segment.pipelines.segment_identity_daily.queries import SegmentsQuery
from pipe_segment.pipelines.segment_identity_daily.table_config import (
    SegmentIdentityDailyTableConfig,
    SegmentIdentityDailyTableDescription,
)
from pipe_segment.pipelines.segment_identity_daily.transforms.core import SummarizeSegmentIdentity
from pipe_segment.version import __version__


def segments_query(config: SegmentIdentityDailyConfig) -> SegmentsQuery:
    """Returns the query that reads the segments of the date range."""
    query = SegmentsQuery(
        source_segments=config.bq_in_segments,
        source_fragments=config.bq_in_fragments,
        start_date=config.start_date,
        end_date=config.end_date,
    )
    return query.with_env(config.jinja_env)


def run(
    config: SegmentIdentityDailyConfig,
    read_from_bigquery_factory: Callable | None = None,
    write_to_bigquery_factory: Callable | None = None,
    bq_client_factory: Callable | None = None,
    **kwargs: Any,
) -> None:
    """Runs the segment-identity-daily pipeline.

    The Beam options come from the project and the labels of the config, the arguments that
    the CLI did not parse, and gfw-common's defaults.

    It waits for the job to finish, unless config.wait_for_job is False. Then it returns once
    the job is submitted, and the table's metadata is not updated, as that happens when the
    job finishes. A job that does not end in DONE raises PipelineNotDoneError.

    Args:
        config:
            The pipeline's configuration.

        read_from_bigquery_factory:
            Builds the transforms that read from BigQuery. Defaults to gfw-common's, mocked if
            config.mock_bq_clients is set.

        write_to_bigquery_factory:
            Builds the transform that writes to BigQuery. Defaults to gfw-common's, mocked if
            config.mock_bq_clients is set.

        bq_client_factory:
            Builds the BigQuery client of the table hooks. Defaults to gfw-common's, mocked
            if config.mock_bq_clients is set.

        **kwargs:
            Apache Beam pipeline options.
    """
    if read_from_bigquery_factory is None:
        read_from_bigquery_factory = ReadFromBigQuery.get_client_factory(
            mocked=config.mock_bq_clients
        )
    if write_to_bigquery_factory is None:
        write_to_bigquery_factory = WriteToBigQueryWrapper.get_client_factory(
            mocked=config.mock_bq_clients
        )
    if bq_client_factory is None:
        bq_client_factory = BigQueryHelper.get_client_factory(mocked=config.mock_bq_clients)

    table_config = SegmentIdentityDailyTableConfig(
        table_id=config.bq_out_segment_identity_daily,
        description=SegmentIdentityDailyTableDescription(
            version=__version__,
            relevant_params={
                "bq_in_segments": config.bq_in_segments,
                "bq_in_fragments": config.bq_in_fragments,
                "start_date": config.start_date,
                "end_date": config.end_date,
            },
        ),
    )

    dag = LinearDag(
        sources=[
            ReadFromBigQuery.from_query(
                segments_query(config),
                label="ReadSegments",
                read_from_bigquery_factory=read_from_bigquery_factory,
                read_from_bigquery_kwargs={"bigquery_job_labels": config.labels},
            )
        ],
        core=SummarizeSegmentIdentity(),
        sinks=(
            WriteToBigQueryWrapper(
                table=table_config.table_id,
                schema=table_config.schema,
                write_to_bigquery_factory=write_to_bigquery_factory,
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                create_disposition=beam.io.BigQueryDisposition.CREATE_NEVER,
            ),
        ),
    )

    pipeline = Pipeline(
        dag=dag,
        pre_hooks=[
            create_table_hook(
                table_config, labels=config.labels, bq_client_factory=bq_client_factory
            ),
            delete_events_hook(
                table_config,
                start_date=config.start_date,
                end_date=config.end_date,
                labels=config.labels,
                bq_client_factory=bq_client_factory,
            ),
        ],
        post_hooks=[
            update_table_hook(
                table_config, labels=config.labels, bq_client_factory=bq_client_factory
            )
        ],
        unparsed_args=config.unknown_unparsed_args,
        wait_for_job=config.wait_for_job,
        project=config.project,
        labels=config.labels,
        **config.unknown_parsed_args,
        **kwargs,
    )

    pipeline.run()
