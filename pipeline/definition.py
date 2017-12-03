import os
import json
from datetime import datetime
import pytz

import apache_beam as beam
from apache_beam import io
from apache_beam import Map
from apache_beam import GroupByKey
from apache_beam import FlatMap
from apache_beam import WindowInto
from apache_beam.transforms import window
from apache_beam.io.gcp.bigquery import BigQuerySink
from apache_beam.io.gcp.bigquery import BigQueryDisposition

from pipe_tools.io import WriteToBigQueryDatePartitioned
from pipe_tools.timestamp import DATE_FORMAT
from pipe_tools.timestamp import timestampFromDatetime
from pipe_tools.timestamp import datetimeFromTimestamp
from pipe_tools.timestamp import TimestampedValueDoFn
from pipe_tools.io.bigquery import parse_table_schema
from pipe_tools.coders import JSONDictCoder

from pipe_template.transforms.source import BigQuerySource
from pipe_template.transforms.sink import Sink
from pipe_template.transforms.segment import Segment
from pipe_template.schemas import segment as segment_schema
from pipe_template.schemas import messages_out as output_schema
from pipe_template.io.gcp import parse_gcp_path
from pipe_template.io.bigquery import QueryBuilder



def asTimestamp(s):
    dt = pytz.UTC.localize(datetime.strptime(s, DATE_FORMAT))
    return timestampFromDatetime(dt)


class PipelineDefinition():
    def __init__(self, options):
        self.options = options

    def _segments_sink(self):
        service, path = parse_gcp_path(self.options.segments_sink)
        schema = segment_schema.build()
        if service == 'table':
            return Sink(
                table=path,
                write_disposition=self.options.sink_write_disposition,
                schema=schema
            )
        elif service == 'query':
            raise RuntimeError("Cannot use a query as a sink")
        else:
            return io.WriteToText(
                file_path_prefix=path,
                coder=JSONDictCoder()
            )

    def _segmeter_params(self):
        if self.options.segmenter_params:
            return json.loads(self.options.segmenter_params)
        else:
            return {}

    def _mesages_source(self):
        service, path = parse_gcp_path(self.options.messages_source)
        schema = parse_table_schema(self.options.messages_schema)

        if service == 'table':
            include_fields = self.options.include_fields.split(',')
            first_date_ts = asTimestamp(self.options.first_date)
            last_date_ts = asTimestamp(self.options.last_date)
            builder = QueryBuilder(table=path, first_date_ts=first_date_ts,
                                   last_date_ts=last_date_ts)
            query = builder.build_query(include_fields=include_fields,
                                        where_sql=self.options.where_sql)
            source = BigQuerySource(query=query)
            schema = builder.filter_table_schema(include_fields=include_fields)
        elif service == 'query':
            assert schema is not None, 'you must provide a schema when the messages source is a query'
            source = BigQuerySource(query=path)
        else:
            source = io.ReadFromText(
                file_pattern=path,
                coder=JSONDictCoder()
            )
        return source, schema

    def _partition_output(self):
        if self.options.partition_output is not None:
            return self.options.partition_output
        else:
            return self.options.first_date is not None

    def _messages_sink(self, messages_source_schema):
        service, path = parse_gcp_path(self.options.messages_sink)

        if service == 'table':
            schema = output_schema.build(messages_source_schema)
            if self._partition_output():
                if not self.options.temp_gcs_location:
                    raise RuntimeError("must supply --temp_gcs_location when writing to date-sharded tables")
                sink = WriteToBigQueryDatePartitioned(
                    temp_gcs_location=self.options.temp_gcs_location,
                    table=path, schema=schema,
                    write_disposition=self.options.messages_write_disposition
                )
            else:
                sink = io.Write(BigQuerySink(table=path, schema=schema,
                                             write_disposition=self.options.messages_write_disposition))

        elif service == 'query':
            raise RuntimeError("Cannot use a query as a sink")
        else:
            sink = io.WriteToText(
                file_path_prefix=path,
                coder=JSONDictCoder()
            )
        return sink

    @staticmethod
    def groupby_fn(msg):
        dt = datetimeFromTimestamp(msg['timestamp']).date()
        key = '%s-%s-%s' % (dt.year, dt.month, msg['mmsi'])
        return (key, msg)

    def build(self, pipeline):

        messages_source, messages_source_schema = self._mesages_source()
        segmenter_params = self._segmeter_params()
        messages_sink = self._messages_sink(messages_source_schema)
        segments_sink = self._segments_sink()

        messages = pipeline | "ReadFromSource" >> messages_source
        messages = messages | 'Timestamp' >> beam.ParDo(TimestampedValueDoFn())

        if self.options.window_size:
            messages = messages | 'window' >> WindowInto(window.FixedWindows(self.options.window_size))

        segmented = (
            messages
            | "AddKey" >> Map(self.groupby_fn)
            | "GroupByKey" >> GroupByKey('mmsi')
            | "Segment" >> Segment(segmenter_params)
        )
        messages = segmented[Segment.OUTPUT_TAG_MESSAGES]
        segments = segmented[Segment.OUTPUT_TAG_SEGMENTS]
        (
            messages
                | beam.ParDo(TimestampedValueDoFn())
                | "WriteMessages" >> messages_sink
        )
        (
            segments
            | "WriteSegments" >> segments_sink
        )
        return pipeline
