from typing import Optional
from dataclasses import dataclass, field
import json
import logging

from google.cloud import bigquery
from google.cloud.exceptions import NotFound

logger = logging.getLogger(__name__)


class Schemas:
    @classmethod
    def json_to_schema_field(cls, field):
        """
        Converts a json dictionary representing a single field in a json schema
        document to an actual SchemaField instance
        """
        subfields = field["fields"] if field["type"].upper() == "RECORD" else []

        return bigquery.SchemaField(
            field['name'],
            field['type'],
            mode=field['mode'],
            description=field['description'],
            fields=(cls.json_to_schema_field(f) for f in subfields)
        )

    @classmethod
    def load_json_schema(cls, schema_path: str):
        """
        Reads a json schema file and converts it to an actual SchemaField list
        """

        with open(schema_path) as json_schema_file:
            json_schema = json.load(json_schema_file)
            return [cls.json_to_schema_field(field) for field in json_schema]


@dataclass(frozen=True)
class SimpleTable:
    """
    Represents an unpartitioned simple table
    """
    table_id: str
    description: str
    schema: list
    clustering_field: Optional[str] = None

    def to_bigquery_table(self):
        """
        Returns a bigquery.Table instance that can be used to create or update
        the remote table at BigQuery.
        """
        table = bigquery.Table(self.table_id, schema=self.schema)
        if self.clustering_field:
            table.clustering_fields = [self.clustering_field]
        table.description = self.description
        return table

    def clear_query(self):
        """
        Returns a query to delete all the data in the table.
        """
        return f"""
                    TRUNCATE TABLE `{self.table_id}`
                """


@dataclass(frozen=True)
class DatePartitionedTable:
    """
    Represents a timestamp-partitioned table with monthly partitions
    """
    table_id: str
    description: str
    schema: list
    partitioning_field: str
    additional_clustering_fields: list = field(default_factory=lambda: [])

    def to_bigquery_table(self):
        """
        Returns a bigquery.Table instance that can be used to create or update
        the remote table at BigQuery
        """
        table = bigquery.Table(self.table_id, schema=self.schema)
        table.time_partitioning = bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.MONTH,
            field=self.partitioning_field,
        )
        table.clustering_fields = [self.partitioning_field, *self.additional_clustering_fields]
        table.description = self.description
        return table

    def clear_query(self, from_date, to_date):
        """
        Returns a query to remove data in the table in a date interval
        """
        return f"""
                    DELETE FROM `{self.table_id}`
                    WHERE date({self.partitioning_field}) >= '{from_date}'
                    AND date({self.partitioning_field}) <= '{to_date}'
                """


class BigQueryHelper:
    def __init__(self, bq_client, labels):
        self.client = bq_client
        self.labels = dict([entry.split("=") for entry in labels])

    def ensure_table_exists(self, table):
        """
        Ensures a table exists, creating it if it doesn't
        """

        logger.info(f"Ensuring table {table.table_id} exists")
        table_definition = table.to_bigquery_table()
        table_definition.labels = self.labels
        result = self.client.create_table(table_definition, exists_ok=True)
        logger.info(f"Table {table.table_id} exists")
        return result

    def run_query(self, query: str):
        """
        Runs a simple, arbitrary query, tagging the query process with the
        labels
        """
        logger.info("Executing query")
        logger.info(f'=====QUERY STARTS======\n{query}\n====QUERY ENDS====')
        rows = self.client.query_and_wait(
            query,
            job_config=bigquery.QueryJobConfig(
                labels=self.labels,
            ),
        )
        logger.info(f"Query job {rows.job_id} done. Total rows {rows.total_rows}.")

    def run_query_into_table(self, *, query: str, table):
        """
        Runs a query and inserts the results into a given table
        """
        logger.info(f'Executing BATCH query, destination {table.table_id}')
        logger.info(f'=====QUERY STARTS======\n{query}\n====QUERY ENDS====')

        rows = self.client.query_and_wait(
            query,
            job_config=bigquery.QueryJobConfig(
                use_query_cache=False,
                priority=bigquery.QueryPriority.BATCH,
                use_legacy_sql=False,
                write_disposition='WRITE_APPEND',
                destination=table.table_id,
                labels=self.labels,
            )
        )
        logger.info(f"Query job {rows.job_id} done. Total rows {rows.total_rows}.")

    def update_table_description(self, table):
        """
        Updates a table description in BigQuery
        """
        bq_table = self.client.get_table(table.table_id)
        bq_table.description = table.description

        logger.info(f"Updating description for table {table.table_id}")
        self.client.update_table(bq_table, ["description"])
        logger.info(f"Description updated for table {table.table_id}.")

    def update_table_labels(self, table):
        """
        Updates the labels of the table.
        """
        bq_table = self.client.get_table(table.table_id)
        bq_table.labels = table.labels

        logger.info(f"Updating labels for table {table.table_id}")
        self.client.update_table(bq_table, ["labels"])
        logger.info(f"labels updated for table {table.table_id}.")

    def fetch_table(self, table_id: str) -> bigquery.Table:
        """
        Returns a bigquery.Table instance for the given table_id, or None if it doesn't exist
        """
        try:
            return self.client.get_table(table_id)
        except NotFound:
            return None
