# This script will create the date-partitioned tables if they dont exists
# and run queries to full-fill those tables with date-sharded tables.
#
# Usage:
# python ./pipe_segment/migration.py

import datetime as dt
import logging
import argparse
import sys
from pipe_segment.utils.bq_tools import DatePartitionedTable, Schemas, BigQueryHelper
from pipe_segment.version import __version__
from pipe_segment.schemas import message_schema, segment_schema
from pipe_segment.transform.fragment import Fragment
from pipe_segment.segment_identity.pipeline import DEST_SEGMENT_IDENTITY_SCHEMA
from pipe_segment.segment_vessel.segment_vessel_daily import (
    SCHEMA_PATH as SCHEMA_SEGMENTVESSELDAILY_PATH,
    description
)
from google.cloud import bigquery

logger = logging.getLogger(__name__)

NAME = "migration-script"
DESCRIPTION = "Use to migrate the segment tables from date-sharded to date-partition."
PROJECT = "world-fishing-827"  # project to bill and where the tables exists.
DATASET_OUT = "scratch_matias_ttl7d"  # dataset to create the date-partitioned tables
DATASET_SOURCE = "pipe_ais_v3_internal"  # dataset where to query the date-sharded tables
DATE_START = "2012-01-01"
QUERY = """
SELECT * FROM `{table_id}_*`
WHERE _TABLE_SUFFIX BETWEEN '{start}' AND '{end}'
"""


def define_tables(dataset_out):
    messages_segmented = DatePartitionedTable(
        table_id=f"{PROJECT}.{dataset_out}.messages_segmented",
        schema=message_schema.message_output_schema,
        description=f"Created by pipe-segment:{__version__}.\n"
                    "Daily satellite messages segmented processed in segment step.",
        partitioning_field="timestamp",
    )
    segments = DatePartitionedTable(
        table_id=f"{PROJECT}.{dataset_out}.segments",
        schema=segment_schema.segment_schema,
        description=f"Created by pipe-segment:{__version__}.\n"
                    "Daily segments processed in segment step.",
        partitioning_field="timestamp",
    )
    fragments = DatePartitionedTable(
        table_id=f"{PROJECT}.{dataset_out}.fragments",
        schema=Fragment.schema,
        description=f"Created by pipe-segment:{__version__}.\n"
                    "Daily fragments processed in segment step.",
        partitioning_field="timestamp",
    )
    segment_identity_daily = DatePartitionedTable(
        table_id=f"{PROJECT}.{dataset_out}.segment_identity_daily",
        description=f"Created by the pipe-segment: {__version__}.\n"
                    "Daily segments identity processed in segment step.",
        schema=DEST_SEGMENT_IDENTITY_SCHEMA,
        partitioning_field="summary_timestamp",
    )
    segment_vessel_daily = DatePartitionedTable(
        table_id=f"{PROJECT}.{dataset_out}.segment_vessel_daily",
        description=description("Migration"),
        schema=Schemas.load_json_schema(SCHEMA_SEGMENTVESSELDAILY_PATH),
        partitioning_field="day",
    )
    return [
        messages_segmented,
        segments,
        fragments,
        segment_identity_daily,
        segment_vessel_daily
    ]


def prepare_output_tables(bq_helper, dataset_out, tables, start, end):
    for table in tables:
        bq_helper.ensure_table_exists(table)
        logging.info(f"Ensures table {table.table_id} is created.")
        query = QUERY.format(
            table_id=table.table_id.replace(dataset_out, DATASET_SOURCE),
            start=start,
            end=end
        )
        logger.info(query)
        bq_helper.run_query_into_table(query=query, table=table)


def migrate(args):
    # Definitions
    logging.getLogger().setLevel(logging.INFO)
    date_start, date_end = [DATE_START, dt.datetime.now().date().isoformat()]

    start = dt.datetime.fromisoformat(date_start)
    end = dt.datetime.fromisoformat(date_end)
    bq_client = bigquery.Client(project=args.billing_project)
    labels = [
        "environment=develop",
        "resource_creator=gcp-composer",
        "project=core_pipeline",
        "version=v3",
        "step=segment",
        "stage=productive"
    ]
    bq_helper = BigQueryHelper(bq_client, labels)
    tables = define_tables(args.dataset_out)
    prepare_output_tables(bq_helper, args.dataset_out, tables,
                          start.strftime('%Y%m%d'), end.strftime('%Y%m%d'))


def run(args) -> int:
    parser = argparse.ArgumentParser(prog=NAME, description=DESCRIPTION)
    parser.add_argument(
        '--dataset_out',
        type=str,
        help="Dataset where to build the date-partitioned tables."
    )
    parser.add_argument(
        '--billing_project',
        type=str,
        help="The project who will be billed to do the migration."
    )

    migrate(parser.parse_args(args))


if __name__ == '__main__':
    run(sys.argv[1:])
