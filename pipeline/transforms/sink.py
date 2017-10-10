from collections import defaultdict

from apache_beam import PTransform
from apache_beam import Map
from apache_beam import io
from pipeline.schemas.output import build as output_schema

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


class ShardedBigQuerySink(io.iobase.Sink):

    def __init__(self, table, schema,
                 create_disposition=io.gcp.bigquery.BigQueryDisposition.CREATE_IF_NEEDED,
                 write_disposition=io.gcp.bigquery.BigQueryDisposition.WRITE_EMPTY,
                 ):
        self.table = table
        self.write_disposition = write_disposition
        self.create_disposition = create_disposition
        self.table_schema = schema
        # TODO: Parse out table_reference

    def initialize_write(self):
        return None

    # TODO: What are init_results and uid for?
    def open_writer(self, init_results, uid):
        return ShardedBigQueryWriter(self)

    def finalize_write(self, init_result, writer_results):
        pass


class ShardedBigQueryWriter(io.iobase.Writer):

    def __init__(self, sink, buffer_size=None):
        self.sink = sink
        self.rows_buffer_flush_threshold = buffer_size or 100
        self.rows_buffer = defaultdict(list)

        # If table schema did not define a project we default to executing project.
        if self.project_id is None and hasattr(sink, 'pipeline_options'):
            self.project_id = (
                sink.pipeline_options.view_as(GoogleCloudOptions).project)

        assert self.project_id is not None

        self.dataset_id = self.sink.table_reference.datasetId
        self.table_id = self.sink.table_reference.tableId

        self.client = io.gcp.bigquery.BigQueryWrapper()

        # self.table = table
        # self.write_disposition = write_disposition
        # self.create_disposition = 'CREATE_IF_NEEDED'
        # self.schema = schema
        # self.rows_buffer_flush_threshold = 100
        #
        # self.rows_buffer = defaultdict(list)
        # self.client = io.gcp.bigquery.BigQueryWrapper()
        #
        # #TODO: parse table id properly and/or accept project_id and dataset_id separately
        # self.project_id = 'world-fishing-827'
        # self.dataset_id, self.table_id = table.split('.')

    def write(self, item):
        shard_key, values = item
        self.rows_buffer[shard_key].extend(values)
        self._flush_rows_buffer(force=False)

    # TODO: do we need __enter__ and __exit__ ?
    def close(self):
        self._flush_rows_buffer(force=True)

    # @property
    # def _schema(self):
    #
    #     def build_table_schema(spec):
    #         schema = io.gcp.internal.clients.bigquery.TableSchema()
    #
    #         for name, type in spec.iteritems():
    #             field = io.gcp.internal.clients.bigquery.TableFieldSchema()
    #             field.name = name
    #             field.type = type
    #             field.mode = 'nullable'
    #             schema.fields.append(field)
    #
    #         return schema
    #
    #     return build_table_schema(self._spec)

    def _flush_rows_buffer(self, force):
        for shard_key, values in self.rows_buffer.items():
            if force or len(values) > self.rows_buffer_flush_threshold:
                table_id = '{}_{}'.format(self.table_id, shard_key)
                # TODO: keep track of dates we've seen and use `self._write_disposition`
                # the first time we see them and 'WRITE_APPEND' after. I'm not sure why
                # we are seeing tables multiple times though.
                self.client.get_or_create_table(
                    self.project_id, self.dataset_id, table_id, self.sink.table_schema,
                    self.sink.create_disposition, self.sink.write_disposition)
                for v in values:
                    assert isinstance(v, dict)
                passed, errors = self.client.insert_rows(
                  project_id=self.project_id, dataset_id=self.dataset_id,
                  table_id=table_id, rows=values)
                del self.rows_buffer[shard_key]
                if not passed:
                    raise RuntimeError('Could not successfully insert rows to BigQuery'
                                   ' table [%s:%s.%s]. Errors: %s'%
                                   (self.project_id, self.dataset_id,
                                    table_id, errors))



def encode_datetime(value):
    return value.strftime('%Y-%m-%d %H:%M:%S.%f UTC')

def encode_date(value):
    return value.strftime('%Y%m%d')


class TestPartitionSink(beam.PTransform):
    def __init__(self, table, write_disposition):
        self.table = table
        self.write_disposition = write_disposition

    def encode(self, item):
        key, values = item
        return key, [{
            'mmsi' : msg['mmsi'],
            'timestamp': encode_datetime(msg['timestamp']),
            } for msg in values]

    spec = {
            "mmsi": "integer",
            "timestamp": "timestamp",
            # "_PARTITIONTIME": "timestamp" # This needed to be present to write to partioned table
        }

    def expand(self, xs):
        return (xs
            | beam.Map(self.encode)
            | io.Write(PartitionedBQSink(
                table=self.table,
                write_disposition=self.write_disposition,
                spec=self.spec
                ))
        )

# Rudimentary test harness...

import argparse
from apache_beam import io


def add_pipeline_defaults(pipeline_args, name):

    defaults = {
        '--project' : 'world-fishing-827',
        '--staging_location' : 'gs://machine-learning-dev-ttl-30d/anchorages/{}/output/staging'.format(name),
        '--temp_location' : 'gs://machine-learning-dev-ttl-30d/anchorages/temp',
        '--setup_file' : './setup.py',
        '--runner': 'DataflowRunner',
        '--max_num_workers' : '200',
        '--job_name': name,
    }

    for name, value in defaults.items():
        if name not in pipeline_args:
            pipeline_args.extend((name, value))


def parse_command_line_args():
    parser = argparse.ArgumentParser()

    parser.add_argument('--name', required=True,
                        help='Name to prefix output and job name if not otherwise specified')

    known_args, pipeline_args = parser.parse_known_args()

    add_pipeline_defaults(pipeline_args, known_args.name)

    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(SetupOptions).save_main_session = True

    return known_args, pipeline_options



query = """
    SELECT mmsi, timestamp FROM
      TABLE_DATE_RANGE([world-fishing-827:pipeline_classify_p_p429_resampling_2.],
                        TIMESTAMP('2016-02-01'), TIMESTAMP('2016-02-28'))
    WHERE mmsi in (211534710, 227099050, 226002780, 235001620)
    LIMIT 100000
    """

def reencode_timestamp(msg):
    msg['timestamp'] = datetime.datetime.strptime(msg['timestamp'], '%Y-%m-%d %H:%M:%S.%f %Z')
    return msg

def run():
    known_args, pipeline_options = parse_command_line_args()


    p = beam.Pipeline(options=pipeline_options)

    (p
        | io.Read(io.gcp.bigquery.BigQuerySource(query=query))
        | beam.Map(lambda x: reencode_timestamp(x))
        | beam.Map(lambda x: (encode_date(x['timestamp']), x))
        | beam.GroupByKey()
        | TestPartitionSink(table='machine_learning_dev_ttl_30d.test_partition_2',
                           write_disposition="WRITE_APPEND")
        )

    result = p.run()
    result.wait_until_finish()

if __name__ == "__main__":
    logging.getLogger().setLevel(logging.DEBUG)
    run()




