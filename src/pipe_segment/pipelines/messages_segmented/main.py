"""Entry point of the messages-segmented pipeline."""

import datetime as dt

from typing import Any, Callable

import apache_beam as beam

from google.cloud import bigquery

from gfw.common.beam.pipeline.base import Pipeline
from gfw.common.beam.pipeline.hooks import (
    create_table_hook,
    delete_events_hook,
    update_table_hook,
)
from gfw.common.beam.transforms import ReadFromBigQuery, WriteToBigQueryWrapper
from gfw.common.bigquery.helper import BigQueryHelper
from gfw.common.bigquery.table_config import TableConfig
from pipe_segment.pipelines.messages_segmented.config import MessagesSegmentedConfig
from pipe_segment.pipelines.messages_segmented.dag import MessagesSegmentedDag
from pipe_segment.pipelines.messages_segmented.hooks import (
    verify_messages_exist_hook,
    verify_prior_segments_hook,
)
from pipe_segment.pipelines.messages_segmented.table_config import (
    FragmentsTableConfig,
    FragmentsTableDescription,
    SatelliteOffsetsTableConfig,
    SatelliteOffsetsTableDescription,
    SegmentedMessagesTableConfig,
    SegmentedMessagesTableDescription,
    SegmentsTableConfig,
    SegmentsTableDescription,
)
from pipe_segment.version import __version__


def table_configs(config: MessagesSegmentedConfig) -> list[TableConfig]:
    """Returns the configs of the tables the pipeline writes.

    The messages, the segments and the fragments come first, in this order, and the satellite
    offsets follow if they are written.
    """
    params = {
        "bq_in_normalized_messages": config.bq_in_normalized_messages,
        "start_date": config.start_date,
        "end_date": config.end_date,
        "incremental": config.incremental,
    }
    tables: list[TableConfig] = [
        SegmentedMessagesTableConfig(
            table_id=config.bq_out_segmented_messages,
            description=SegmentedMessagesTableDescription(
                version=__version__,
                relevant_params={
                    **params,
                    "max_timing_offset_s": config.max_timing_offset_s,
                    "bad_hour_padding": config.bad_hour_padding,
                    "ssvid_filter_query": config.ssvid_filter_query,
                },
            ),
        ),
        SegmentsTableConfig(
            table_id=config.bq_out_segments,
            description=SegmentsTableDescription(
                version=__version__,
                relevant_params={**params, "bq_in_fragments": config.in_fragments_table},
            ),
        ),
        FragmentsTableConfig(
            table_id=config.bq_out_fragments,
            description=FragmentsTableDescription(version=__version__, relevant_params=params),
        ),
    ]
    if config.bq_out_sat_offsets:
        tables.append(
            SatelliteOffsetsTableConfig(
                table_id=config.bq_out_sat_offsets,
                description=SatelliteOffsetsTableDescription(
                    version=__version__,
                    relevant_params={
                        "bq_in_sat_offset_messages": config.bq_in_sat_offset_messages,
                        "bq_in_norad_to_receiver": config.bq_in_norad_to_receiver,
                        "bq_in_sat_positions": config.bq_in_sat_positions,
                        "start_date": config.start_date,
                        "end_date": config.end_date,
                    },
                ),
            )
        )

    return tables


def sink(table_config: TableConfig, write_to_bigquery_factory: Callable) -> beam.PTransform:
    """Returns the transform that appends rows to a table that already exists."""
    return write_to_bigquery_factory(
        table=table_config.table_id,
        schema={"fields": table_config.schema},
        create_disposition=beam.io.BigQueryDisposition.CREATE_NEVER,
    )


def pre_hooks(
    config: MessagesSegmentedConfig,
    tables: list[TableConfig],
    bq_client_factory: Callable[..., bigquery.Client],
) -> list[Callable[[Pipeline], None]]:
    """Returns the hooks that get the tables ready, in the order to run them.

    The tables are created first, as the checks read them (on the first run they do not exist).
    Then the checks run, and the date range is deleted last, so a failed check deletes nothing.
    Only an incremental run depends on the data written by previous runs, so only it is checked.
    """
    labels = config.labels
    hooks = [
        create_table_hook(table, labels=labels, bq_client_factory=bq_client_factory)
        for table in tables
    ]

    if config.incremental:
        hooks.extend(
            [
                verify_messages_exist_hook(
                    config.bq_in_normalized_messages,
                    config.start_date,
                    config.end_date - dt.timedelta(days=1),
                    labels=labels,
                    bq_client_factory=bq_client_factory,
                ),
                verify_prior_segments_hook(
                    config.in_segments_table,
                    config.in_fragments_table,
                    config.start_date - dt.timedelta(days=1),
                    labels=labels,
                    bq_client_factory=bq_client_factory,
                ),
            ]
        )

    hooks.extend(
        delete_events_hook(
            table,
            start_date=config.start_date,
            end_date=config.end_date,
            labels=labels,
            bq_client_factory=bq_client_factory,
        )
        for table in tables
    )
    return hooks


def run(
    config: MessagesSegmentedConfig,
    read_from_bigquery_factory: Callable | None = None,
    write_to_bigquery_factory: Callable | None = None,
    bq_client_factory: Callable | None = None,
    **kwargs: Any,
) -> None:
    """Runs the messages-segmented pipeline.

    The Beam options come from the project and the labels of the config, the arguments that
    the CLI did not parse, and gfw-common's defaults.

    It waits for the job to finish, unless config.wait_for_job is False. Then it returns once
    the job is submitted, and the tables' metadata is not updated, as that happens when the
    job finishes. A job that does not end in DONE raises PipelineNotDoneError.

    Args:
        config:
            The pipeline's configuration.

        read_from_bigquery_factory:
            Builds the transform that reads the messages from BigQuery. Defaults to
            gfw-common's, mocked if config.mock_bq_clients is set.

        write_to_bigquery_factory:
            Builds the transforms that write to BigQuery. Defaults to gfw-common's, mocked if
            config.mock_bq_clients is set.

        bq_client_factory:
            Builds the BigQuery client of the hooks and of the transforms. Defaults to
            gfw-common's, mocked if config.mock_bq_clients is set.

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

    tables = table_configs(config)
    messages_table, segments_table, fragments_table, *_ = tables

    dag = MessagesSegmentedDag(
        config,
        messages_sink=sink(messages_table, write_to_bigquery_factory),
        segments_sink=sink(segments_table, write_to_bigquery_factory),
        fragments_sink=sink(fragments_table, write_to_bigquery_factory),
        read_from_bigquery_factory=read_from_bigquery_factory,
    )

    pipeline = Pipeline(
        dag=dag,
        pre_hooks=pre_hooks(config, tables, bq_client_factory),
        post_hooks=[
            update_table_hook(table, labels=config.labels, bq_client_factory=bq_client_factory)
            for table in tables
        ],
        unparsed_args=config.unknown_unparsed_args,
        wait_for_job=config.wait_for_job,
        project=config.project,
        labels=config.labels,
        **config.unknown_parsed_args,
        **kwargs,
    )

    pipeline.run()
