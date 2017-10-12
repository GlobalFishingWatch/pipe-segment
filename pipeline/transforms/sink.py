import json
import uuid
import logging
import time
from collections import defaultdict

import apache_beam as beam
from apache_beam import PTransform
from apache_beam import io
from apache_beam.utils import retry

from pipeline.schemas.output import build as output_schema
from apache_beam.io.gcp.internal.clients import bigquery


class Sink(PTransform):
    def __init__(self, table, schema, write_disposition=None):
        self.table = table
        self.write_disposition = write_disposition
        self.schema = schema

    def encode_datetime(self, value):
        return value.strftime('%Y-%m-%d %H:%M:%S.%f UTC')

    def encode_datetime_fields(self, x):
        x['timestamp'] = self.encode_datetime(x['timestamp'])
        return x

    def expand(self, xs):
        big_query_sink = io.gcp.bigquery.BigQuerySink(
            table=self.table,
            write_disposition=self.write_disposition,
            schema=self.schema,
        )

        return (
            xs
            | io.Write(big_query_sink)
        )


class WriteToDatePartitionedBigQuery(PTransform):
    """
    A transform that will write to a date-partitioned bigquery table
    The date partition is determined per element by calling partition_fn()
    with each element.   partition_fn() should return a string formatted date
    'YYYYMMDD'
    """
    def __init__(self, table, schema, partition_fn,
                 write_disposition=None, intra_day_shards=None):
        self.table = table
        self.write_disposition = write_disposition
        self.partition_fn = partition_fn
        # store schema as dict so that it will be serializeable
        self.table_schema_dict = io.gcp.bigquery.WriteToBigQuery.table_schema_to_dict(parse_table_schema(schema))
        self.intra_day_shards = intra_day_shards or 1

    @property
    def table_schema(self):
        return parse_table_schema(self.table_schema_dict)

    def expand(self, pcoll):
        bq_sink = ShardedBigQuerySink(
            table=self.table,
            write_disposition=self.write_disposition,
            schema=self.table_schema,
        )

        return (
            pcoll
            | beam.Map(lambda row: (self.partition_fn(row), row))
            | "GroupByPartition" >> beam.GroupByKey('partition')
            | io.Write(bq_sink)
        )


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
    elif isinstance(schema, bigquery.TableSchema):
        return schema
    elif isinstance(schema, basestring):
        # try to parse json into dict
        try:
            schema = json.loads(schema)
        except ValueError, e:
            pass

    if isinstance(schema, basestring):
        # if it is still a string, then it must not be json.  Assume it is string representation
        return io.gcp.bigquery.WriteToBigQuery.get_table_schema_from_string(schema)
    elif isinstance(schema, dict):
        # either it came in as a dict or it got converted from json earlier
        return io.gcp.bigquery.parse_table_schema_from_json(json.dumps(schema))
    else:
        raise TypeError('Unexpected schema argument: %s.' % schema)



# Supply a partition function, use that to extract dates from the data
#
# Provide a transform class that uses the partition function to do a group by key and then
# invokes the Partitioned Sink
#
# sink.initialize_write should create a temporary BQ dataset
#
# sink.open writer writes to one temporary table per date
#  in the temporary data set and returns the list of temporary tables
#  The date is encoded in the temp table name
#
# sink.finalize_write uses the BQ wrapper to create the final table and runs a union
# query to combine all the temp tables with the same date into each date partition in the
# output table

from apitools.base.protorpclite import messages as _messages

# Time partitioning is not supported in the V2 api that is included in apache beam
# TODO: figure out how to write time partitioned tables
# TODO: Also look at using JobConfigurationTableCopy instead of a sql query to combine the temp tables
class TimePartitioning(_messages.Message):
    type = _messages.StringField(1)

# subclass BigQueryWrapper so we can add a few things
class BigQueryWrapper(io.gcp.bigquery.BigQueryWrapper):
    def __init__(self, **kwargs):
        super(BigQueryWrapper, self).__init__(**kwargs)

    @retry.with_exponential_backoff(
        num_retries=io.gcp.bigquery.MAX_RETRIES,
        retry_filter=retry.retry_on_server_errors_and_timeout_filter)
    def start_query_job_with_dest_table(self, project_id, query, destination_table,
                                        use_legacy_sql=True, dry_run=False):
        job_id = uuid.uuid4().hex
        job_reference = bigquery.JobReference(jobId=job_id, projectId=project_id)
        table_reference = io.gcp.bigquery._parse_table_reference(table=destination_table)
        # time_partitioning = TimePartitioning(type='DAY')
        request = bigquery.BigqueryJobsInsertRequest(
            projectId=project_id,
            job=bigquery.Job(
                configuration=bigquery.JobConfiguration(
                    dryRun=dry_run,
                    query=bigquery.JobConfigurationQuery(
                        query=query,
                        useLegacySql=use_legacy_sql,
                        allowLargeResults=True,
                        destinationTable=table_reference,
                        writeDisposition=io.gcp.bigquery.BigQueryDisposition.WRITE_TRUNCATE
                    )),
                jobReference=job_reference))
        response = self.client.jobs.Insert(request)
        return response.jobReference.jobId


class ShardedBigQuerySink(io.iobase.Sink):

    def __init__(self, table, schema, dataset=None, project=None,
                 create_disposition=io.gcp.bigquery.BigQueryDisposition.CREATE_IF_NEEDED,
                 write_disposition=io.gcp.bigquery.BigQueryDisposition.WRITE_EMPTY,
                 ):
        self.table_reference = io.gcp.bigquery._parse_table_reference(table, dataset, project)
        self.write_disposition = write_disposition
        self.create_disposition = create_disposition

        self.project_id = project or self.table_reference.projectId

        # # TODO: Parse out table_reference
        # self.project_id = 'world-fishing-827'
        # self.dataset_id = 'scratch_paul'

        # store schema as dict so that it will be serializeable
        self.table_schema_dict = io.gcp.bigquery.WriteToBigQuery.table_schema_to_dict(parse_table_schema(schema))

    @property
    def table_schema(self):
        return parse_table_schema(self.table_schema_dict)

    def _get_combine_tables_sql(self, dataset, tables):
        tables = ["[%s.%s]" % (dataset, t) for t in tables]
        return "select * from %s" % ",".join(tables)

    def initialize_write(self):
        # Create temporary dataset
        client = BigQueryWrapper()
        client.create_temporary_dataset(self.project_id)
        return client._get_temp_table(self.project_id).datasetId

    def open_writer(self, temp_dataset, uid):
        # write to temporary tables in temporary data set
        # tables names written are returned from the writer's close() method
        # and passed in to finalize_write() in writer_results
        table_prefix='temp_%s' % str(uid).replace('-','_')
        return ShardedBigQueryWriter(self, dataset=temp_dataset, table_prefix=table_prefix)

    def finalize_write(self, temp_dataset, writer_results):
        # bundle up temporary tables in writer_results and insert them into
        # the target table in the appropriate date partitions
        client = BigQueryWrapper()
        tables_by_partition = defaultdict(list)
        for result in writer_results:
            for table_id in result:
                # table_id ends with _YYYYMMDD
                partition = table_id[-8:]
                tables_by_partition[partition].append(table_id)
        job_ids = set()
        for partition, tables in tables_by_partition.items():
            query = self._get_combine_tables_sql(temp_dataset, tables)
            table_ref = io.gcp.bigquery._parse_table_reference(
                table = "{}_{}".format(self.table_reference.tableId, partition),
                dataset = self.table_reference.datasetId,
                project = self.table_reference.projectId
            )
            job_id = client.start_query_job_with_dest_table(project_id=self.project_id,
                                                            query=query,
                                                            destination_table=table_ref)
            logging.info('copying records from %s to %s', tables, table_ref)
            job_ids.add(job_id)

        complete_job_ids = set()
        while job_ids != complete_job_ids:
            for job_id in job_ids:
                response = client._get_query_results(self.project_id, job_id)
                if response.jobComplete:
                    complete_job_ids.add(job_id)
                else:
                    # The jobComplete field can be False if the query request times out
                    # (default is 10 seconds). Note that this is a timeout for the query
                    # request not for the actual execution of the query in the service.  If
                    # the request times out we keep trying. This situation is quite possible
                    # if the query will return a large number of rows.
                    logging.info('Waiting on response from job: %s ...', job_id)
                time.sleep(1.0)

        # Delete the temporary dataset
        client._delete_dataset(self.project_id, temp_dataset, True)


class ShardedBigQueryWriter(io.iobase.Writer):

    def __init__(self, sink, dataset=None, table_prefix=None, buffer_size=None):
        self.sink = sink
        self.rows_buffer_flush_threshold = buffer_size or 100
        self.rows_buffer = defaultdict(list)

        self.project_id = self.sink.project_id
        self.dataset_id = dataset or self.sink.dataset_id
        self.table_prefix = table_prefix or self.sink.table

        self.client = io.gcp.bigquery.BigQueryWrapper()
        self.tables_written = set()

    def write(self, item):
        shard_key, values = item
        self.rows_buffer[shard_key].extend(values)
        self._flush_rows_buffer(force=False)

    def close(self):
        self._flush_rows_buffer(force=True)
        result = self.tables_written
        self.tables_written = set()
        return result

    def _flush_rows_buffer(self, force):
        for shard_key, values in self.rows_buffer.items():
            if force or len(values) > self.rows_buffer_flush_threshold:
                table_id = '{}_{}'.format(self.table_prefix, shard_key)
                self.client.get_or_create_table(
                    self.project_id, self.dataset_id, table_id, self.sink.table_schema,
                    self.sink.create_disposition, self.sink.write_disposition)
                for v in values:
                    assert isinstance(v, dict)
                passed, errors = self.client.insert_rows(
                  project_id=self.project_id, dataset_id=self.dataset_id,
                  table_id=table_id, rows=values)
                self.tables_written.add(table_id)
                del self.rows_buffer[shard_key]
                if not passed:
                    raise RuntimeError('Could not successfully insert rows to BigQuery'
                                   ' table [%s:%s.%s]. Errors: %s'%
                                   (self.project_id, self.dataset_id,
                                    table_id, errors))


#
# def encode_datetime(value):
#     return value.strftime('%Y-%m-%d %H:%M:%S.%f UTC')
#
# def encode_date(value):
#     return value.strftime('%Y%m%d')
#
#
# class TestPartitionSink(beam.PTransform):
#     def __init__(self, table, write_disposition):
#         self.table = table
#         self.write_disposition = write_disposition
#
#     def encode(self, item):
#         key, values = item
#         return key, [{
#             'mmsi' : msg['mmsi'],
#             'timestamp': encode_datetime(msg['timestamp']),
#             } for msg in values]
#
#     spec = {
#             "mmsi": "integer",
#             "timestamp": "timestamp",
#             # "_PARTITIONTIME": "timestamp" # This needed to be present to write to partioned table
#         }
#
#     def expand(self, xs):
#         return (xs
#             | beam.Map(self.encode)
#             | io.Write(PartitionedBQSink(
#                 table=self.table,
#                 write_disposition=self.write_disposition,
#                 spec=self.spec
#                 ))
#         )
#
# # Rudimentary test harness...
#
# import argparse
# from apache_beam import io
#
#
# def add_pipeline_defaults(pipeline_args, name):
#
#     defaults = {
#         '--project' : 'world-fishing-827',
#         '--staging_location' : 'gs://machine-learning-dev-ttl-30d/anchorages/{}/output/staging'.format(name),
#         '--temp_location' : 'gs://machine-learning-dev-ttl-30d/anchorages/temp',
#         '--setup_file' : './setup.py',
#         '--runner': 'DataflowRunner',
#         '--max_num_workers' : '200',
#         '--job_name': name,
#     }
#
#     for name, value in defaults.items():
#         if name not in pipeline_args:
#             pipeline_args.extend((name, value))
#
#
# def parse_command_line_args():
#     parser = argparse.ArgumentParser()
#
#     parser.add_argument('--name', required=True,
#                         help='Name to prefix output and job name if not otherwise specified')
#
#     known_args, pipeline_args = parser.parse_known_args()
#
#     add_pipeline_defaults(pipeline_args, known_args.name)
#
#     pipeline_options = PipelineOptions(pipeline_args)
#     pipeline_options.view_as(SetupOptions).save_main_session = True
#
#     return known_args, pipeline_options
#
#
#
# query = """
#     SELECT mmsi, timestamp FROM
#       TABLE_DATE_RANGE([world-fishing-827:pipeline_classify_p_p429_resampling_2.],
#                         TIMESTAMP('2016-02-01'), TIMESTAMP('2016-02-28'))
#     WHERE mmsi in (211534710, 227099050, 226002780, 235001620)
#     LIMIT 100000
#     """
#
# def reencode_timestamp(msg):
#     msg['timestamp'] = datetime.datetime.strptime(msg['timestamp'], '%Y-%m-%d %H:%M:%S.%f %Z')
#     return msg
#
# def run():
#     known_args, pipeline_options = parse_command_line_args()
#
#
#     p = beam.Pipeline(options=pipeline_options)
#
#     (p
#         | io.Read(io.gcp.bigquery.BigQuerySource(query=query))
#         | beam.Map(lambda x: reencode_timestamp(x))
#         | beam.Map(lambda x: (encode_date(x['timestamp']), x))
#         | beam.GroupByKey()
#         | TestPartitionSink(table='machine_learning_dev_ttl_30d.test_partition_2',
#                            write_disposition="WRITE_APPEND")
#         )
#
#     result = p.run()
#     result.wait_until_finish()
#
# if __name__ == "__main__":
#     logging.getLogger().setLevel(logging.DEBUG)
#     run()
#
#
#
#
