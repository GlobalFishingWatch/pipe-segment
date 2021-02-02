import logging
import ujson
from datetime import timedelta
from datetime import datetime
from apitools.base.py.exceptions import HttpError

import apache_beam as beam
from apache_beam.runners import PipelineState
from apache_beam.io.gcp.internal.clients.bigquery import TableFieldSchema
from apache_beam.options.pipeline_options import GoogleCloudOptions
from apache_beam.options.pipeline_options import StandardOptions
from apache_beam import io
from apache_beam.transforms.window import TimestampedValue
from apache_beam.io.gcp.internal.clients import bigquery

from pipe_tools.timestamp import TimestampedValueDoFn
from pipe_tools.timestamp import datetimeFromTimestamp
from pipe_tools.timestamp import timestampFromDatetime
from pipe_tools.utils.timestamp import as_timestamp
from pipe_tools.io.bigquery import parse_table_schema
from pipe_tools.io import WriteToBigQueryDatePartitioned

from pipe_segment.options.segment import SegmentOptions
from pipe_segment.transform.segment import Segment
from pipe_segment.transform.normalize import NormalizeDoFn
from pipe_segment.io.gcp import GCPSource
from pipe_segment.io.gcp import GCPSink

PAD_PREV_DAYS = 1

def safe_dateFromTimestamp(ts):
    if ts is None:
        return None
    return datetimeFromTimestamp(ts).date()

def parse_date_range(s):
    # parse a string YYYY-MM-DD,YYYY-MM-DD into 2 timestamps
    return list(map(as_timestamp, s.split(',')) if s is not None else (None, None))

def offset_timestamp(ts, **timedelta_args):
    if ts is None:
        return ts
    dt = datetimeFromTimestamp(ts) + timedelta(**timedelta_args)
    return timestampFromDatetime(dt)

def is_first_batch(pipeline_start_ts, first_date_ts):
    return pipeline_start_ts == first_date_ts

def filter_by_ssvid_predicate(obj, valid_ssvid_set):
    return obj['ssvid'] in valid_ssvid_set

class LogMapper(object):
    first_item = True
    which_item = 0

    def log_first_item(self, obj):
        if self.first_item:
            logging.warn("First Item: %s", obj)
            self.first_item = False
        return obj

    def log_nth_item(self, obj, n):
        if self.which_item == n:
            logging.warn("%sth Item: %s", n, obj)
            self.which_item += 1
        return obj

    def log_first_item_keys(self, obj):
        if self.first_item:
            logging.warn("First Item's Keys: %s", obj.keys())
            self.first_item = False
        return obj


def get_query_windows(start_date, end_date):
    start_window = start_date
    while start_window <= end_date:
        end_window = min(start_window + timedelta(days=800), end_date)
        yield start_window, end_window
        start_window = end_window + timedelta(days=1)


def satellite_offset_iter(start_date, end_date):
    template = """
        WITH

        position_messages as (
          SELECT *,
                 ABS(TIMESTAMP_DIFF(LAG(timestamp) OVER 
                     (PARTITION BY ssvid ORDER BY timestamp), timestamp, SECOND)) next_dt,
                 ABS(TIMESTAMP_DIFF(LEAD(timestamp) OVER 
                     (PARTITION BY ssvid ORDER BY timestamp), timestamp, SECOND)) prev_dt,
                 TIMESTAMP_TRUNC(timestamp, HOUR) hour,
                 ROW_NUMBER() OVER (PARTITION BY ssvid, receiver, EXTRACT(MINUTE FROM timestamp)
                                    ORDER by ABS(EXTRACT(SECOND FROM timestamp) - 30)) rn
          FROM `pipe_ais_sources_v20190222.normalized_spire_*`
          WHERE _table_suffix BETWEEN "{start_window:%Y%m%d}" AND "{end_window:%Y%m%d}"
            AND lat IS NOT NULL AND lon IS NOT NULL 
            AND ABS(lat) <= 90 AND ABS(lon) <= 180
         ),
         

         distance_from_satellite_table as (
            SELECT
              a.msgid,
              TIMESTAMP_TRUNC(a.timestamp, HOUR) hour,
              st_distance(st_geogpoint(a.lon,a.lat), st_geogpoint(c.lon,c.lat))/1000 distance_from_sat_km,
              altitude/1000 as sat_altitude_km,
              a.receiver receiver,
              c.lat as sat_lat,
              c.lon as sat_lon
            FROM
              position_messages a
            LEFT JOIN (
              SELECT
                norad_id,
                receiver
              FROM
                `world-fishing-827.gfw_research_precursors.norad_to_receiver_v20200127` ) b
            ON a.receiver = b.receiver
            LEFT JOIN (
              SELECT
                avg(lat) lat,
                avg(lon) lon,
                avg(altitude) altitude,
                timestamp,
                norad_id
              FROM
                `satellite_positions_v20190208.satellite_positions_one_second_resolution_*`
             WHERE _table_suffix BETWEEN "{start_window:%Y%m%d}" AND "{end_window:%Y%m%d}" 
              GROUP BY
                norad_id, timestamp) c
            ON a.timestamp = c.timestamp
            AND b.norad_id = c.norad_id
            ),

        median_dist_from_sat as 

        (
        select hour, receiver, avg(distance_from_sat_km) avg_distance_from_sat_km, 
        med_dist_from_sat_km from
        (select hour, receiver, distance_from_sat_km,
        percentile_cont(distance_from_sat_km, 0.5) over (partition by receiver, hour) AS med_dist_from_sat_km
        from distance_from_satellite_table)
        group by hour, receiver, med_dist_from_sat_km
        ),
         
         
        base AS (
          SELECT ssvid,
                 hour,
                 timestamp,
                 receiver,
                 lat,
                 lon,
                 speed,
                 course
          FROM position_messages
          WHERE speed between 5 AND 10
            AND lat is not null
            AND lon is not null
            AND course is not null
            AND course != 360
            AND speed  < 102.3
            AND abs(lat) <= 90
            AND abs(lon) <= 180
            AND receiver_type = 'satellite'
            AND type != 'AIS.27'
            -- AND source = 'spire' 
            AND rn = 1 -- only 1 point per ssvid, receiver pair per minute
        ),
        hours as (
          SELECT receiver, hour, COUNT(*) pings
          FROM base
          GROUP BY receiver, hour
        ),
        pairs AS (
          SELECT
            ssvid,
            hour,
            timestamp_diff(b.timestamp, a.timestamp, millisecond) / 1000.0 AS dt, -- dt is in seconds
            a.lat AS lat1, b.lat AS lat2,
            a.lon AS lon1, b.lon AS lon2,
            0.5 * (a.speed + b.speed) AS speed,
            0.5 * (a.course + b.course) AS course,
            a.receiver AS receiver1, b.receiver AS receiver2,
            ROW_NUMBER() OVER (PARTITION BY ssvid, a.receiver, b.receiver, hour 
                               ORDER BY timestamp_diff(b.timestamp, a.timestamp, millisecond)) rn
          FROM base AS a
          JOIN base AS b
          USING (hour, ssvid) -- Joining using hour limits the range and chops off some offsets at edges
          WHERE a.receiver != b.receiver
           AND ABS(timestamp_diff(b.timestamp, a.timestamp, millisecond) / 1000.0) < 600
           AND cos(3.14159 / 180 * (a.course - b.course)) > 0.8 -- very little difference in course
        ),
        -- This collects ping pairs with timestamp withing 10 minutes of each other.
        -- The two pairs also have similar course (from above), so we expect that
        -- boats are "well behaved" over this period.
        close_pairs AS (
            SELECT *,
                   (lon2 - lon1) * cos(0.5 * (lat1 + lat2) * 3.14159 / 180) * 60 AS dx,
                   (lat2 - lat1) * 60 AS dy,
                   SUM(IF(rn = 1, 1, 0)) over(partition by receiver1, receiver2) AS pair_count
            FROM pairs
            WHERE abs(dt) < 600 
              AND rn = 1  -- only use one ping per hour for each (ssvid, receiver1, receiver2) combo
        ),
         
        _distances_1 AS (
          SELECT * except (pair_count, dx, dy),
                 SQRT(dx * dx + dy * dy) AS distance,
                 ATAN2(dx, dy) AS implied_course_rads
          FROM close_pairs
          WHERE pair_count >= 10
        ),
        _distances_2 AS (
          SELECT *, 
                 COS(course  * 3.14159 / 180 - implied_course_rads) AS cos_delta
          FROM _distances_1
        ),
        distances AS (
            SELECT * except(distance),
                   -- `sign` here takes care of case where boats implied course and course are ~180 deg apart
                   SIGN(cos_delta) * distance AS signed_distance
            FROM _distances_2
            -- only use cases where implied course ~agree or are ~opposite 
            WHERE ABS(cos_delta) > 0.8
        ),
        -- Compute the expected dts
        delta_ts AS (
            SELECT *, signed_distance / speed * 60 * 60 AS expected_dt,
            FROM distances
        ),
        grouped AS (
          SELECT *
          FROM (
            SELECT hour,
                   percentile_cont(dt - expected_dt, 0.5) over (partition by receiver1, receiver2, hour) AS dt,
                   receiver1,
                   receiver2
            FROM delta_ts
          )
        GROUP BY receiver1, receiver2, dt, hour
        ), 
        time_offset_by_hour_by_satellite AS (
          SELECT * 
          FROM (
            SELECT receiver1 AS receiver,
                   hour,
                   percentile_cont(dt, 0.5) over (partition by receiver1, hour) AS dt
            FROM grouped
          )
          GROUP BY receiver, hour, dt
        ),
        safe_time_offset_by_hour_by_satellite AS (
            SELECT receiver, hour,
                   GREATEST(dt, 
                            IFNULL(LAG(dt) OVER(PARTITION BY receiver ORDER BY hour), 0),
                            IFNULL(LEAD(dt) OVER(PARTITION BY receiver ORDER BY hour), 0)), dt
            FROM time_offset_by_hour_by_satellite
        )


        SELECT * except (hour), CAST(UNIX_MILLIS(hour) AS FLOAT64) / 1000  AS hour
        FROM time_offset_by_hour_by_satellite
        LEFT JOIN hours 
        USING (hour, receiver)
        left join median_dist_from_sat
        using(hour, receiver)
        ORDER BY receiver, hour
    """
    for start_window, end_window in get_query_windows(start_date, end_date):
        query = template.format(start_window=start_window, end_window=end_window)
        yield io.Read(io.gcp.bigquery.BigQuerySource(query=query, use_standard_sql=True))



def make_bad_hours_set(bad_hours_iter, pad_hours):
    bad_hours = set()
    for rcvr, hours in bad_hours_iter:
        for offset in range(-pad_hours, pad_hours + 1):
            hr = offset_timestamp(hr, hours=offset)
            bad_hours.add((rcvr, hr))
    return bad_hours

def filter_sat_times(msg, bad_hours):
    dt = datetimeFromTimestamp(msg['timestamp'])
    hr = datetime(dt.year, dt.month, dt.day, dt.hour)
    key = (msg['receiver'], datetimeFromTimestamp(hr))
    return key not in bad_hours

class SegmentPipeline:
    def __init__(self, options):
        self.options = options.view_as(SegmentOptions)
        self.date_range = parse_date_range(self.options.date_range)
        self._message_source_map = None

    @property
    def sat_offset_schema(self):
        schema = bigquery.TableSchema()

        def add_field(name, field_type, mode='REQUIRED'):
            field = bigquery.TableFieldSchema()
            field.name = name
            field.type = field_type
            field.mode = mode
            schema.fields.append(field)

        add_field('hour', 'timestamp')
        add_field('receiver',  'STRING')
        add_field('dt', 'FLOAT')
        add_field('pings', 'INTEGER')
        add_field('avg_distance_from_sat_km', 'FLOAT')
        add_field('med_dist_from_sat_km', 'FLOAT')

        return schema

    @property
    def sat_offset_sink(self):
        sink_table = self.options.sat_offset_dest
        if not sink_table.startswith('bq://'):
            raise ValueError('only BigQuery supported as a destination for `sat_offset_dest, must begine with "bq://"')
        sink_table = sink_table[5:]
        if ':' not in sink_table:
          sink_table = self.cloud_options.project + ':' + sink_table
        print(sink_table)
        sink = WriteToBigQueryDatePartitioned(table=sink_table,
                       schema=self.sat_offset_schema,
                       temp_gcs_location=self.temp_gcs_location,
                       temp_shards_per_day=self.options.temp_shards_per_day,
                       write_disposition="WRITE_EMPTY")
        # TODO:
                       # "WRITE_TRUNCATE")
        return sink

    @property
    def message_source_map(self):
        # creating a GCPSource requires calls to the bigquery API if we are
        # reading from bigquery, so only do this once.
        if not self._message_source_map:
            first_date_ts, last_date_ts = self.date_range
            #If the first_date_ts is the first day of data, then, don't look back
            if not is_first_batch(as_timestamp(self.options.pipeline_start_date),first_date_ts):
                first_date_ts = offset_timestamp(first_date_ts, days=-PAD_PREV_DAYS)
            if self.options.look_ahead:
                last_date_ts = offset_timestamp(last_date_ts, days=self.options.look_ahead)
            gcp_paths = self.options.source.split(',')
            self._message_source_map = {}
            for gcp_path in gcp_paths:
                s = GCPSource(gcp_path=gcp_path,
                               first_date_ts=first_date_ts,
                               last_date_ts=last_date_ts)
                self._message_source_map[gcp_path] = s

        return self._message_source_map

    def message_sources(self, pipeline):
        gcp_paths = set(self.options.sat_source.split(','))

        def compose(idx, source):
            return pipeline | "OtherSource%i" % idx >> source

        return [compose(i, message_source_map[p]) for (i, p) in enumerate(gcp_paths)]


    @property
    def message_input_schema(self):
        schema = self.options.source_schema
        if schema is None:
            # no explicit schema provided. Try to find one in the source(s)
            schemas = [s.schema for s in self._message_source_map.values() 
                                    if s.schema is not None]
            schema = schemas[0] if schemas else None
        return parse_table_schema(schema)

    @property
    def message_output_schema(self):
        schema = self.message_input_schema

        field = TableFieldSchema()
        field.name = "seg_id"
        field.type = "STRING"
        field.mode="NULLABLE"
        schema.fields.append(field)

        field = TableFieldSchema()
        field.name = "n_shipname"
        field.type = "STRING"
        field.mode="NULLABLE"
        schema.fields.append(field)

        field = TableFieldSchema()
        field.name = "n_callsign"
        field.type = "STRING"
        field.mode="NULLABLE"
        schema.fields.append(field)

        field = TableFieldSchema()
        field.name = "n_imo"
        field.type = "INTEGER"
        field.mode="NULLABLE"
        schema.fields.append(field)

        return schema


    @property
    def segmenter_params(self):
        return ujson.loads(self.options.segmenter_params)

    @property
    def temp_gcs_location(self):
        return self.options.view_as(GoogleCloudOptions).temp_location

    @staticmethod
    def groupby_fn(msg):
        return (msg['ssvid'], msg)

    @property
    def message_sink(self):
        sink = GCPSink(gcp_path=self.options.msg_dest,
                       schema=self.message_output_schema,
                       temp_gcs_location=self.temp_gcs_location,
                       temp_shards_per_day=self.options.temp_shards_per_day)
        return sink

    @property
    def segment_source(self):
        if self.date_range[0] is None:
            return beam.Create([])

        dt = datetimeFromTimestamp(self.date_range[0])
        ts = timestampFromDatetime(dt - timedelta(days=1))

        try:
            source = GCPSource(gcp_path=self.options.seg_dest,
                             first_date_ts=ts,
                             last_date_ts=ts)
        except HttpError as exn:
            logging.warn("Segment source not found: %s %s" % (self.options.seg_dest, dt))
            if exn.status_code == 404:
                return beam.Create([])
            else:
                raise
        return source

    @property
    def bad_satellite_time_sources(self, pipeline):
        def compose(idx, source):
            return pipeline | "Source%i" % idx >> source

        sources = [self._message_source_map[x] for x in self.bad_satellite_time_sources]

        return (compose (idx, source) for idx, source in enumerate(sources))

    def segment_sink(self, schema, table):
        sink = GCPSink(gcp_path=table,
                       schema=schema,
                       temp_gcs_location=self.temp_gcs_location,
                       temp_shards_per_day=self.options.temp_shards_per_day)
        return sink


    def pipeline(self):
        # Note that Beam appears to treat str(x) and unicode(x) as distinct
        # for purposes of CoGroupByKey, so both messages and segments should be
        # stringified or neither. 
        pipeline = beam.Pipeline(options=self.options)

        start_date = safe_dateFromTimestamp(self.date_range[0])
        end_date = safe_dateFromTimestamp(self.date_range[1])

        satellite_offsets = (
            [pipeline | 'MsgSource{}'.format(ndx) >> src for (ndx, src) in 
                            enumerate(satellite_offset_iter(start_date, end_date))]
            | "MergeOffsets" >> beam.Flatten() 
        )

        ( satellite_offsets 
            | "AddTimestamp" >> beam.Map(lambda x: TimestampedValue(x, x['hour']))
            | "WriteSatOffsets" >> self.sat_offset_sink
        )

        # bad_satellite_hours = (
        #     satellite_offsets 
        #     | beam.Filter(lambda x : abs(x['dt']) > options.max_timing_offset) # Imp
        #     | beam.Map(lambda x : (x['receiver'], x['hour']))
        # )

        # bad_hours = make_bad_hours_set(beam.pvalue.AsIter(bad_satellite_hours), 
        #                                 pad_hours=options.bad_hour_padding)

        # sources = (
        #     self.message_sources(pipeline)
        #     | "MergeMessages" >> beam.Flatten()
        #     | "FilterBadTimes" >> beam.Filter(filter_sat_times, bad_hours)
        # )


        # if self.options.ssvid_filter_query:
        #     valid_ssivd_set = set(beam.pvalue.AsIter(
        #         messages
        #         | GCPSource(gcp_path=self.options.ssvid_filter_query)
        #         | beam.Map(lambda x: (x['ssvid']))
        #         ))
        #     messages = (
        #         messages
        #         | beam.Filter(filter_by_ssvid_predicate, valid_ssivd_set)
        #     )

        # messages = (   
        #     messages
        #     | "Normalize" >> beam.ParDo(NormalizeDoFn())
        #     | "MessagesAddKey" >> beam.Map(self.groupby_fn)
        # )

        # segments = (
        #     pipeline
        #     | "ReadSegments" >> self.segment_source
        #     | "RemoveClosedSegments" >> beam.Filter(lambda x: not x['closed'])
        #     | "SegmentsAddKey" >> beam.Map(self.groupby_fn)
        # )

        # args = (
        #     {'messages' : messages, 'segments' : segments}
        #     | 'GroupByKey' >> beam.CoGroupByKey()
        # )

        # segmenter = Segment(start_date=safe_dateFromTimestamp(self.date_range[0]),
        #                     end_date=safe_dateFromTimestamp(self.date_range[1]),
        #                     segmenter_params=self.segmenter_params, 
        #                     look_ahead=self.options.look_ahead)

        # segmented = args | "Segment" >> segmenter

        # messages = segmented[segmenter.OUTPUT_TAG_MESSAGES]
        # segments = segmented[segmenter.OUTPUT_TAG_SEGMENTS]
        # (
        #     messages
        #     | "TimestampMessages" >> beam.ParDo(TimestampedValueDoFn())
        #     | "WriteMessages" >> self.message_sink
        # )
        # (
        #     segments
        #     | "TimestampSegments" >> beam.ParDo(TimestampedValueDoFn())
        #     | "WriteSegments" >> self.segment_sink(segmenter.segment_schema, 
        #                                            self.options.seg_dest)
        # )
        # if self.options.legacy_seg_v1_dest:
        #     segments_v1 = segmented[segmenter.OUTPUT_TAG_SEGMENTS_V1]
        #     (
        #         segments_v1
        #         | "TimestampOldSegments" >> beam.ParDo(TimestampedValueDoFn())
        #         | "WriteOldSegments" >> self.segment_sink(segmenter.segment_schema_v1, 
        #                                                self.options.legacy_seg_v1_dest)
        #     )
        return pipeline

    def run(self):
        return self.pipeline().run()


def run(options):

    pipeline = SegmentPipeline(options)
    result = pipeline.run()

    success_states = set([PipelineState.DONE])

    if pipeline.options.wait_for_job or options.view_as(StandardOptions).runner == 'DirectRunner':
        result.wait_until_finish()
    else:
        success_states.add(PipelineState.RUNNING)
        success_states.add(PipelineState.UNKNOWN)
        success_states.add(PipelineState.PENDING)

    logging.info('returning with result.state=%s' % result.state)
    return 0 if result.state in success_states else 1
