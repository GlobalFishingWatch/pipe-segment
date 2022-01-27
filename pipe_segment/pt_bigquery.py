import ujson
import six
from datetime import datetime

import apache_beam as beam
from apache_beam.io.gcp.bigquery import WriteToBigQuery
from apache_beam.io.gcp.bigquery_tools import parse_table_schema_from_json
import apache_beam.io.gcp.internal.clients.bigquery as bq
from apache_beam.utils import retry
from apache_beam.io.gcp.internal.clients.bigquery import TableSchema


from pipe_tools.timestamp import timestampFromDatetime
from pipe_tools.timestamp import datetimeFromTimestamp


MAX_RETRIES = 8


def parse_table_schema(schema):
    """
    Accepts a BigQuery tableschema as a string, dict (from json), or bigquery.TabelSchema and returns
    a bigquery.TableSchema

    String Format

    "[FIELD]:[DATA_TYPE],[FIELD]:[DATA_TYPE]"

    dict format

    {
      "fields": [
        {
          "name": "[FIELD]",
          "type": "[DATA_TYPE]"
        },
        {
          "name": "[FIELD]",
          "type": "[DATA_TYPE]"
        }
    ]}

    see https://cloud.google.com/bigquery/data-types
    see https://cloud.google.com/bigquery/docs/schemas#specifying_a_schema_file


    """
    if schema is None:
        return schema
    elif isinstance(schema, bq.TableSchema):
        return schema
    elif isinstance(schema, six.string_types):
        # try to parse json into dict
        try:
            schema = ujson.loads(schema)
        except ValueError as e:
            pass

    if isinstance(schema, six.string_types):
        # if it is still a string, then it must not be json.  Assume it is string representation
        return WriteToBigQuery.get_table_schema_from_string(schema)
    elif isinstance(schema, dict):
        # either it came in as a dict or it got converted from json earlier
        return parse_table_schema_from_json(ujson.dumps(schema))
    else:
        raise TypeError("Unexpected schema argument: %s." % schema)


# decode a bigquery table ref string PROJECT:DATASET.TABLE_ID
# into a TableReferenece.   You can supply each component separately,
# or all in one string
def decode_table_ref(table, dataset=None, project=None):
    return beam.io.gcp.bigquery_tools.parse_table_reference(table, dataset, project)


# encode a TableReference to a string representation
def encode_table_ref(table_ref, use_legacy_sql):
    if table_ref.projectId:
        if use_legacy_sql:
            return "{}:{}.{}".format(
                table_ref.projectId, table_ref.datasetId, table_ref.tableId
            )
        else:
            return "{}.{}.{}".format(
                table_ref.projectId, table_ref.datasetId, table_ref.tableId
            )
    else:
        return "{}.{}".format(table_ref.datasetId, table_ref.tableId)


# subclass BigQueryWrapper so we can add a few things
class BigQueryWrapper(beam.io.gcp.bigquery_tools.BigQueryWrapper):
    def __init__(self, **kwargs):
        super(BigQueryWrapper, self).__init__(**kwargs)

    @retry.with_exponential_backoff(
        num_retries=MAX_RETRIES,
        retry_filter=retry.retry_on_server_errors_and_timeout_filter,
    )
    def load_table(
        self,
        job_id,
        project_id,
        table_ref,
        table_schema,
        gcs_urls,
        create_disposition,
        write_disposition,
    ):

        job_ref = bq.JobReference(jobId=job_id, projectId=project_id)
        request = bq.BigqueryJobsInsertRequest(
            projectId=project_id,
            job=bq.Job(
                configuration=bq.JobConfiguration(
                    load=bq.JobConfigurationLoad(
                        createDisposition=create_disposition,
                        destinationTable=table_ref,
                        schema=table_schema,
                        sourceFormat="NEWLINE_DELIMITED_JSON",
                        sourceUris=gcs_urls,
                        writeDisposition=write_disposition,
                    )
                ),
                jobReference=job_ref,
            ),
        )

        response = self.client.jobs.Insert(request)
        return response.jobReference.jobId

    @retry.with_exponential_backoff(
        num_retries=MAX_RETRIES,
        retry_filter=retry.retry_on_server_errors_and_timeout_filter,
    )
    def get_job_status(self, project_id, job_id):
        request = bq.BigqueryJobsGetRequest(jobId=job_id, projectId=project_id)
        return self.client.jobs.Get(request)

    def get_table_schema(self, project_id, dataset_id, table_id):
        table = self.get_table(project_id, dataset_id, table_id)
        return table.schema


class QueryHelper:
    """
    Helper class that builds date range queries and fetches table schemas
    """

    def __init__(
        self,
        table,
        dataset=None,
        project=None,
        first_date_ts=None,
        last_date_ts=None,
        use_legacy_sql=True,
        test_client=None,
    ):
        self.first_date_ts = first_date_ts
        self.last_date_ts = last_date_ts

        self.table_ref = decode_table_ref(table, dataset, project)
        table_id = self.table_ref.tableId

        self.use_legacy_sql = use_legacy_sql

        if first_date_ts is not None:
            assert (
                last_date_ts is not None
            ), "Must supply both first_date and last_date, or neither"
            dt = datetimeFromTimestamp(first_date_ts)
            table_id = "{}{}".format(table_id, dt.strftime("%Y%m%d"))

        client = BigQueryWrapper(client=test_client)
        self._table_info = client.get_table(
            self.table_ref.projectId, self.table_ref.datasetId, table_id
        )

    @staticmethod
    def _date_to_sql_timestamp(date, use_legacy_sql=True):
        if isinstance(date, six.string_types):
            return "TIMESTAMP({})".format(date)
        elif isinstance(date, datetime):
            timestamp = timestampFromDatetime(date)
        else:
            # assume that date is already a timestamp
            timestamp = date
        ts_fn = "SEC_TO_TIMESTAMP" if use_legacy_sql else "TIMESTAMP_SECONDS"
        return "{}({})".format(ts_fn, int(timestamp))

    def _format_table(self):
        if self.use_legacy_sql:
            table = "[{}]".format(encode_table_ref(self.table_ref, self.use_legacy_sql))
            if self.is_date_sharded:
                table_params = dict(
                    table=table,
                    first_date=self._date_to_sql_timestamp(
                        self.first_date_ts, self.use_legacy_sql
                    ),
                    last_date=self._date_to_sql_timestamp(
                        self.last_date_ts, self.use_legacy_sql
                    ),
                )
                table = "TABLE_DATE_RANGE({table}, {first_date}, {last_date})".format(
                    **table_params
                )
        else:
            table = "`{}{}`".format(
                encode_table_ref(self.table_ref, self.use_legacy_sql),
                "*" if self.is_date_sharded else "",
            )
        return table

    def _format_where_clause(self, where_sql):
        if self.is_date_sharded and not self.use_legacy_sql:
            template = "_TABLE_SUFFIX BETWEEN FORMAT_TIMESTAMP('%Y%m%d', {}) AND FORMAT_TIMESTAMP('%Y%m%d', {}) AND {}"
            first_date = self._date_to_sql_timestamp(
                self.first_date_ts, self.use_legacy_sql
            )
            last_date = self._date_to_sql_timestamp(
                self.last_date_ts, self.use_legacy_sql
            )
            result = template.format(first_date, last_date, where_sql or "True")
        else:
            result = where_sql or "True"

        return result

    def build_query(self, include_fields=None, where_sql=None):
        fields = ",".join(include_fields or "*")
        table = self._format_table()
        where = self._format_where_clause(where_sql)
        return "SELECT {fields} FROM {table} WHERE {where}".format(
            fields=fields, table=table, where=where
        )

    def filter_table_schema(self, include_fields=None):
        if include_fields is None:
            schema = self.table_schema
        else:
            schema = TableSchema()
            schema.fields = [
                field
                for field in self.table_schema.fields
                if field.name in include_fields
            ]
        return schema

    @property
    def table_info(self):
        return self._table_info

    @property
    def table_schema(self):
        return self._table_info.schema

    @property
    def is_date_sharded(self):
        return self.first_date_ts is not None
