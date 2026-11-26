"""Entry point of the messages-segmented pipeline."""

import datetime as dt

from typing import Any, Callable

import apache_beam as beam

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

    params = {
        "bq_in_normalized_messages": config.bq_in_normalized_messages,
        "start_date": config.start_date,
        "end_date": config.end_date,
        "incremental": config.incremental,
    }
    messages_table = SegmentedMessagesTableConfig(
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
    )
    segments_table = SegmentsTableConfig(
        table_id=config.bq_out_segments,
        description=SegmentsTableDescription(
            version=__version__,
            relevant_params={**params, "bq_in_fragments": config.in_fragments_table},
        ),
    )
    fragments_table = FragmentsTableConfig(
        table_id=config.bq_out_fragments,
        description=FragmentsTableDescription(version=__version__, relevant_params=params),
    )
    tables: list[TableConfig] = [messages_table, segments_table, fragments_table]

    # The satellite offsets are written only if their table is given, and they come last.
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

    # The sinks append to tables that already exist: the pre-hooks create them.
    messages_sink, segments_sink, fragments_sink, *satellite_offsets_sinks = [
        write_to_bigquery_factory(
            table=table.table_id,
            schema={"fields": table.schema},
            create_disposition=beam.io.BigQueryDisposition.CREATE_NEVER,
        )
        for table in tables
    ]
    satellite_offsets_sink = None
    if satellite_offsets_sinks:
        satellite_offsets_sink = satellite_offsets_sinks[0]

    dag = MessagesSegmentedDag(
        config,
        messages_sink=messages_sink,
        segments_sink=segments_sink,
        fragments_sink=fragments_sink,
        satellite_offsets_sink=satellite_offsets_sink,
        read_from_bigquery_factory=read_from_bigquery_factory,
    )

    # The tables are created first, as the checks read them (on the first run they do not exist).
    # Then the checks run, and the date range is deleted last, so a failed check deletes nothing.
    labels = config.labels
    pre_hooks = [
        create_table_hook(table, labels=labels, bq_client_factory=bq_client_factory)
        for table in tables
    ]

    # Only an incremental run depends on the data written by previous runs, so only it is checked.
    if config.incremental:
        pre_hooks.extend(
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

    pre_hooks.extend(
        delete_events_hook(
            table,
            start_date=config.start_date,
            end_date=config.end_date,
            labels=labels,
            bq_client_factory=bq_client_factory,
        )
        for table in tables
    )

    pipeline = Pipeline(
        dag=dag,
        pre_hooks=pre_hooks,
        post_hooks=[
            update_table_hook(table, labels=labels, bq_client_factory=bq_client_factory)
            for table in tables
        ],
        unparsed_args=config.unknown_unparsed_args,
        wait_for_job=config.wait_for_job,
        project=config.project,
        labels=labels,
        **config.unknown_parsed_args,
        **kwargs,
    )

    pipeline.run()
