import logging
import ujson

import apache_beam as beam
from apache_beam.runners import PipelineState
from apache_beam.io.gcp.internal.clients.bigquery import TableFieldSchema
from apache_beam.options.pipeline_options import GoogleCloudOptions
from apache_beam.options.pipeline_options import StandardOptions
from apache_beam.transforms.window import TimestampedValue
from apache_beam.io.gcp.internal.clients import bigquery

from .timestamp import TimestampedValueDoFn
from .timestamp import datetimeFromTimestamp
from .timestamp import as_timestamp
from pipe_tools.io.bigquery import parse_table_schema
from pipe_tools.io import WriteToBigQueryDatePartitioned

from pipe_segment.options.fragment import FragmentOptions
from pipe_segment.transform.invalid_values import filter_invalid_values
from pipe_segment.transform.fragment import Fragment
from pipe_segment.transform.satellite_offsets import SatelliteOffsets
from pipe_segment.transform.normalize import NormalizeDoFn
from pipe_segment.transform.filter_bad_satellite_times import FilterBadSatelliteTimes
from pipe_segment.transform.read_messages import ReadMessages

from pipe_segment.io.gcp import GCPSource
from pipe_segment.io.gcp import GCPSink

message_input_schema = {
    "fields": [
        {
            "description": "A unique id, we try using the spire raw id but sometimes it is not unique.",
            "mode": "NULLABLE",
            "name": "msgid",
            "type": "STRING",
        },
        {
            "description": "spire/orbcomm used on posterior steps when both spire and orbcomm are merge to know where they come from.",
            "mode": "NULLABLE",
            "name": "source",
            "type": "STRING",
        },
        {
            "description": "The AIS messages, the format is [AIS.type]",
            "mode": "NULLABLE",
            "name": "type",
            "type": "STRING",
        },
        {
            "description": "The Specific Source Vessel ID, in this case the MMSI",
            "mode": "NULLABLE",
            "name": "ssvid",
            "type": "STRING",
        },
        {
            "description": "The timestap that indicates when the message was transmitted.",
            "mode": "NULLABLE",
            "name": "timestamp",
            "type": "TIMESTAMP",
        },
        {
            "description": "The longitude included in the spire message.",
            "mode": "NULLABLE",
            "name": "lon",
            "type": "FLOAT",
        },
        {
            "description": "The latitude included in the spire message.",
            "mode": "NULLABLE",
            "name": "lat",
            "type": "FLOAT",
        },
        {
            "description": "The speed in knots included in the spire json decoded message.",
            "mode": "NULLABLE",
            "name": "speed",
            "type": "FLOAT",
        },
        {
            "description": "The course included in the spire json decoded message.",
            "mode": "NULLABLE",
            "name": "course",
            "type": "FLOAT",
        },
        {
            "description": "The heading included in the spire json decoded message.",
            "mode": "NULLABLE",
            "name": "heading",
            "type": "FLOAT",
        },
        {
            "description": "The shipname included in the spire json decoded message.",
            "mode": "NULLABLE",
            "name": "shipname",
            "type": "STRING",
        },
        {
            "description": "The callsign included in the spire json decoded message.",
            "mode": "NULLABLE",
            "name": "callsign",
            "type": "STRING",
        },
        {
            "description": "The destination included in the spire json decoded message.",
            "mode": "NULLABLE",
            "name": "destination",
            "type": "STRING",
        },
        {
            "description": "The imo included in the spire json decoded message.",
            "mode": "NULLABLE",
            "name": "imo",
            "type": "STRING",
        },
        {
            "description": "The shiptype included in the spire message using the shiptype.json match.",
            "mode": "NULLABLE",
            "name": "shiptype",
            "type": "STRING",
        },
        {
            "description": "terrestrial or satellite. Obtained through the collection_type in the spire json decoded message.",
            "mode": "NULLABLE",
            "name": "receiver_type",
            "type": "STRING",
        },
        {
            "description": "The source from spire json decoded message.",
            "mode": "NULLABLE",
            "name": "receiver",
            "type": "STRING",
        },
        {
            "description": "The length of the vessel from the SPIRE message.",
            "mode": "NULLABLE",
            "name": "length",
            "type": "FLOAT",
        },
        {
            "description": "The width of the vessel from the SPIRE message.",
            "mode": "NULLABLE",
            "name": "width",
            "type": "FLOAT",
        },
        {
            "description": "The navigational status.",
            "mode": "NULLABLE",
            "name": "status",
            "type": "INTEGER",
        },
    ]
}


def safe_dateFromTimestamp(ts):
    if ts is None:
        return None
    return datetimeFromTimestamp(ts).date()


def parse_date_range(s):
    # parse a string YYYY-MM-DD,YYYY-MM-DD into 2 timestamps
    return list(map(as_timestamp, s.split(",")) if s is not None else (None, None))


def filter_by_ssvid_predicate(obj, valid_ssvid_set):
    return obj["ssvid"] in valid_ssvid_set


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


class FragmentPipeline:
    def __init__(self, options):
        self.options = options.view_as(FragmentOptions)
        self.date_range = parse_date_range(self.options.date_range)
        self._message_source_list = None

    @property
    def sat_offset_schema(self):
        schema = bigquery.TableSchema()

        def add_field(name, field_type, mode="REQUIRED"):
            field = bigquery.TableFieldSchema()
            field.name = name
            field.type = field_type
            field.mode = mode
            schema.fields.append(field)

        add_field("hour", "timestamp")
        add_field("receiver", "STRING")
        add_field("dt", "FLOAT")
        add_field("pings", "INTEGER")
        add_field("avg_distance_from_sat_km", "FLOAT")
        add_field("med_dist_from_sat_km", "FLOAT")

        return schema

    @property
    def sat_offset_sink(self):
        sink_table = self.options.sat_offset_dest
        if not sink_table.startswith("bq://"):
            raise ValueError(
                "only BigQuery supported as a destination for `sat_offset_dest`,"
                ' must begin with "bq://"'
            )
        sink_table = sink_table[5:]
        if ":" not in sink_table:
            sink_table = self.cloud_options.project + ":" + sink_table
        print(sink_table)
        sink = WriteToBigQueryDatePartitioned(
            table=sink_table,
            schema=self.sat_offset_schema,
            temp_gcs_location=self.temp_gcs_location,
            temp_shards_per_day=self.options.temp_shards_per_day,
            write_disposition="WRITE_TRUNCATE",
        )
        return sink

    @property
    def message_source_list(self):
        gcp_paths = self.options.source.split(",")
        start_date = safe_dateFromTimestamp(self.date_range[0])
        end_date = safe_dateFromTimestamp(self.date_range[1])
        self._message_source_list = []
        for gcp_path in gcp_paths:
            s = ReadMessages(
                source=gcp_path,
                start_date=start_date,
                end_date=end_date,
                ssvid_filter_query=self.options.ssvid_filter_query,
            )
            self._message_source_list.append(s)

        #         )        # # creating a GCPSource requires calls to the bigquery API if we are
        # # reading from bigquery, so only do this once.
        # if not self._message_source_list:
        #     first_date_ts, last_date_ts = self.date_range
        #     gcp_paths = self.options.source.split(",")
        #     self._message_source_list = []
        #     for gcp_path in gcp_paths:
        #         s = GCPSource(
        #             gcp_path=gcp_path,
        #             first_date_ts=first_date_ts,
        #             last_date_ts=last_date_ts,
        #         )
        #         self._message_source_list.append(s)

        return self._message_source_list

    def message_sources(self, pipeline):
        def compose(idx, source):
            return pipeline | "Source%i" % idx >> source

        return (
            compose(idx, source) for idx, source in enumerate(self.message_source_list)
        )

    @property
    def message_input_schema(self):
        # schemas = [s.schema for s in self.message_source_list if s.schema is not None]
        # schema = schemas[0] if schemas else None
        return message_input_schema

    @property
    def message_output_schema(self):
        schema = self.message_input_schema.copy()
        schema["fields"] = schema["fields"][:]

        schema["fields"].extend(
            [
                {"name": "frag_id", "type": "STRING", "mode": "NULLABLE"},
                {"name": "n_shipname", "type": "STRING", "mode": "NULLABLE"},
                {"name": "n_callsign", "type": "STRING", "mode": "NULLABLE"},
                {"name": "n_imo", "type": "STRING", "mode": "NULLABLE"},
            ]
        )

        return schema

    @property
    def fragmenter_params(self):
        return ujson.loads(self.options.fragmenter_params)

    @property
    def temp_gcs_location(self):
        return self.options.view_as(GoogleCloudOptions).temp_location

    @staticmethod
    def groupby_fn(msg):
        return ((msg["ssvid"], safe_dateFromTimestamp(msg["timestamp"])), msg)

    @property
    def message_sink(self):
        sink = GCPSink(
            gcp_path=self.options.msg_dest,
            schema=self.message_output_schema,
            temp_gcs_location=self.temp_gcs_location,
            temp_shards_per_day=self.options.temp_shards_per_day,
        )
        return sink

    def fragment_sink(self, schema, table):
        sink = GCPSink(
            gcp_path=table,
            schema=schema,
            temp_gcs_location=self.temp_gcs_location,
            temp_shards_per_day=self.options.temp_shards_per_day,
        )
        return sink

    def pipeline(self):
        pipeline = beam.Pipeline(options=self.options)

        start_date = safe_dateFromTimestamp(self.date_range[0])
        end_date = safe_dateFromTimestamp(self.date_range[1])

        messages = (
            self.message_sources(pipeline)
            | "MergeMessages" >> beam.Flatten()
            | "FilterInvalidValues" >> beam.Map(filter_invalid_values)
        )

        if self.options.sat_source:
            satellite_offsets = pipeline | SatelliteOffsets(start_date, end_date)

            if self.options.sat_offset_dest:
                (
                    satellite_offsets
                    | "AddTimestamp"
                    >> beam.Map(lambda x: TimestampedValue(x, x["hour"]))
                    | "WriteSatOffsets" >> self.sat_offset_sink
                )

            messages = messages | FilterBadSatelliteTimes(
                satellite_offsets,
                max_timing_offset_s=self.options.max_timing_offset_s,
                bad_hour_padding=self.options.bad_hour_padding,
            )

        messages = (
            messages
            | "Normalize" >> beam.ParDo(NormalizeDoFn())
            | "MessagesAddKey" >> beam.Map(self.groupby_fn)
            | "GroupBySsvidAndDay" >> beam.GroupByKey()
        )

        fragmenter = Fragment(
            # TODO: WRONG. Segmenter shouldn't take start and end date
            # it should only process current one day ever.
            start_date=safe_dateFromTimestamp(self.date_range[0]),
            end_date=safe_dateFromTimestamp(self.date_range[1]),
            fragmenter_params=self.fragmenter_params,
        )

        fragmented = messages | "Fragment" >> fragmenter

        messages = fragmented[fragmenter.OUTPUT_TAG_MESSAGES]
        fragments = fragmented[fragmenter.OUTPUT_TAG_FRAGMENTS]
        (
            messages
            | "TimestampMessages" >> beam.ParDo(TimestampedValueDoFn())
            | "WriteMessages" >> self.message_sink
        )

        (
            fragments
            | "TimestampFragments" >> beam.ParDo(TimestampedValueDoFn())
            | "WriteFragments"
            >> self.fragment_sink(fragmenter.fragment_schema, self.options.frag_dest)
        )
        return pipeline

    def run(self):
        return self.pipeline().run()


def run(options):

    pipeline = FragmentPipeline(options)
    result = pipeline.run()

    success_states = set([PipelineState.DONE])

    if (
        pipeline.options.wait_for_job
        or options.view_as(StandardOptions).runner == "DirectRunner"
    ):
        result.wait_until_finish()
    else:
        success_states.add(PipelineState.RUNNING)
        success_states.add(PipelineState.UNKNOWN)
        success_states.add(PipelineState.PENDING)

    logging.info("returning with result.state=%s" % result.state)
    return 0 if result.state in success_states else 1
