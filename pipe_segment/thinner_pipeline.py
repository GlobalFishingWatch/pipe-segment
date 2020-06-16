import logging
import ujson
from datetime import timedelta
from apitools.base.py.exceptions import HttpError

import apache_beam as beam
from apache_beam import io
from apache_beam.runners import PipelineState
from apache_beam.io.gcp.internal.clients.bigquery import TableFieldSchema
from apache_beam.options.pipeline_options import GoogleCloudOptions
from apache_beam.options.pipeline_options import StandardOptions
from apache_beam.transforms.window import TimestampedValue

from pipe_tools.timestamp import TimestampedValueDoFn
from pipe_tools.timestamp import datetimeFromTimestamp
from pipe_tools.timestamp import timestampFromDatetime
from pipe_tools.utils.timestamp import as_timestamp
from pipe_tools.io.bigquery import parse_table_schema
from pipe_tools.io.bigquery import QueryHelper
from pipe_tools.io import WriteToBigQueryDatePartitioned

from pipe_segment.options.thinner import ThinnerOptions
from pipe_segment.transform.stitcher import Stitch
from pipe_segment.transform.normalize import NormalizeDoFn
from pipe_segment.io.gcp import GCPSource
from pipe_segment.io.gcp import GCPSink
from pipe_segment.transform.segment import Segment



def parse_date_range(s):
    # parse a string YYYY-MM-DD,YYYY-MM-DD into 2 timestamps
    return list(map(as_timestamp, s.split(',')) if s is not None else (None, None))




class StitcherPipeline:


    dropped_fields = [
        'shipname', 'callsign', 'imo',
        'shiptype', 'destination', 
        'length', 'width', # TODO: all at the segment level
        'n_shipname', 'n_callsign', 'n_imo', # TODO: also at the segment level
        'status', # TODO: does anyone use status? (is it trustworthy)
        'heading' # TODO: does anyone use heading (often missing)    
    ]


    def __init__(self, options):
        self.options = options.view_as(ThinnerOptions)
        self.cloud_options = options.view_as(GoogleCloudOptions)
        self.date_range = parse_date_range(self.options.date_range)
        self._message_input_schema = None

    @property
    def temp_gcs_location(self):
        return self.options.view_as(GoogleCloudOptions).temp_location

    @property
    def message_output_schema(self):
        schema = self.get_message_input_schema()
        schema.fields = [field for field in schema.fields if field.name not in self.dropped_fields]

        field = TableFieldSchema()
        field.name = "track_id"
        field.type = "STRING"
        field.mode="NULLABLE"
        schema.fields.append(field)

        return schema


    def get_query_windows(self):
        start_date, end_date = [datetimeFromTimestamp(x) for  x in  self.date_range]

        start_window = start_date
        while start_window <= end_date:
            end_window = min(start_window + timedelta(days=800), end_date)
            yield start_window, end_window
            start_window = end_window + timedelta(days=1)


    def get_message_input_schema(self):
        start_window, end_window = next(self.get_query_windows())
        dataset, table = self.options.msg_source.split('.')
        helper = QueryHelper(table=table,
                              dataset=dataset,
                              project=self.cloud_options.project,
                              first_date_ts=timestampFromDatetime(start_window),
                              last_date_ts=timestampFromDatetime(end_window))
        print(helper.build_query())
        return parse_table_schema(helper.table_schema)
   
    @property
    def message_input_schema(self):
        if self.message_input_schema is None:
            self._message_input_schema  = self.get_message_input_schema()
        return self._message_input_schema

    def message_source_iter(self):
        template = """
            WITH 
            -- Grab the `track id`s and `aug_seg_id`s from the track table so that we can plot only
            -- points on the tracks. `aug_seg_id`s are the normal seg_ids augmented with the date, so
            -- segments that extend across multiple days will have different aug_seg_ids for each day.
            track_id as (
              select seg_id as aug_seg_id, track_id
              from `{track_table}`
              cross join unnest (seg_ids) as seg_id
              group by aug_seg_id, track_id
            ),

            -- Grab the messages from big query and add an aug_seg_id field which is constructed from
            -- the seg_id and the date.
            source_w_aug_seg_ids as (
                select 
                    CAST(UNIX_MILLIS(timestamp) AS FLOAT64) / 1000  AS timestamp,
                    * except (
                    timestamp,
                    {dropped_fields}
                ),
                       concat(seg_id, '-', format_date('%F', date(timestamp))) aug_seg_id -- TODO: better names
                from `{msg_table}*`
                where _TABLE_SUFFIX between "{start_date:%Y%m%d}" and "{end_date:%Y%m%d}"
            )

            -- Join the message source to the `track_id` table, matching
            -- up messages to their `track_id` using `aug_seg_id`. 
            select * except (aug_seg_id)
            from source_w_aug_seg_ids a
            join (select * from track_id)
            using (aug_seg_id)
        """
        dropped_fields = ',\n                    '.join(self.dropped_fields)
        for start_window, end_window in self.get_query_windows():
            query = template.format(track_table=self.options.track_source, msg_table=self.options.msg_source,
                                    dropped_fields=dropped_fields, start_date=start_window, end_date=end_window)
            yield io.Read(io.gcp.bigquery.BigQuerySource(query=query, use_standard_sql=True))


    def message_sink(self):
        sink_table = self.options.msg_sink
        if ':' not in sink_table:
          sink_table = self.cloud_options.project + ':' + sink_table
        sink = WriteToBigQueryDatePartitioned(table=sink_table,
                       schema=self.message_output_schema,
                       temp_gcs_location=self.temp_gcs_location,
                       temp_shards_per_day=self.options.temp_shards_per_day,
                       write_disposition="WRITE_TRUNCATE")
        return sink

    def thin_tracks(self, item):
        track_id, messages = item
        messages = sorted(messages, key=lambda x: x['timestamp'])
        if not messages:
            return
        interval = timedelta(minutes=1)
        last_ts = datetimeFromTimestamp(messages[0]['timestamp'])
        yield messages[0]
        for msg in messages[1:]:
            ts = datetimeFromTimestamp(msg['timestamp'])
            if ts - last_ts >= interval:
                yield msg
                last_ts = ts


    def pipeline(self):

        pipeline = beam.Pipeline(options=self.options)

        (
            [pipeline | 'MsgSource{}'.format(ndx) >> src for (ndx, src) in enumerate(self.message_source_iter())]
            | "MergeMessages" >> beam.Flatten()
            | "AddKey" >> beam.Map(lambda x : (x['track_id'], x))
            | "Group" >> beam.GroupByKey()
            | "Thin" >> beam.FlatMap(self.thin_tracks)
            | "AddTimestamp" >> beam.Map(lambda x: TimestampedValue(x, x['timestamp']))
            | "WriteMessages" >> self.message_sink()
        )

        return pipeline

    def run(self):
        return self.pipeline().run()


def run(options):

    pipeline = StitcherPipeline(options)
    result = pipeline.run()

    success_states = set([PipelineState.DONE])

    if pipeline.options.wait_for_job or options.view_as(StandardOptions).runner == 'DirectRunner':
        result.wait_until_finish()
    else:
        success_states.add(PipelineState.RUNNING)
        success_states.add(PipelineState.UNKNOWN)

    logging.info('returning with result.state=%s' % result.state)
    return 0 if result.state in success_states else 1
