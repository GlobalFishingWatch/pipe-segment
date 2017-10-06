import os

import apache_beam as beam
from apache_beam import io
# from apache_beam.io import FileSystems
from apache_beam import Map
from apache_beam import GroupByKey
from apache_beam import FlatMap
from apache_beam import WindowInto
from apache_beam.transforms import window

from pipeline.transforms.source import BigQuerySource
from pipeline.transforms.segment import Segment
from pipeline.transforms.sink import Sink
from pipeline.coders import AddTimestampDoFn
from pipeline.coders import Timestamp2DatetimeDoFn
from pipeline.coders import Datetime2TimestampDoFn
from pipeline.schemas import segment as segment_schema
from pipeline.schemas import output as output_schema

from coders import JSONCoder


class PipelineDefinition():
    def __init__(self, options):
        self.options = options

    def _source(self, option_value):
        if self.options.sourcefile:
            # Source is a file containing new line delimited json
            source = io.ReadFromText(
                file_pattern=self.options.sourcefile,
                coder=JSONCoder()
            )
        elif self.options.sourcequery:
            # Source is a bigquery query
            source = Source(query=self.options.sourcequery)
        return source

    def _messages_sink(self, option_value):
        if self.options.local:
            sink = io.WriteToText(
                file_path_prefix=self.options.sink,
                coder=JSONCoder()
            )
        elif self.options.remote:
            sink = Sink(
                table=self.options.sink,
                write_disposition=self.options.sink_write_disposition,
            )
        return sink

    def _parse_source_sink(self, path):
        scheme = io.filesystems.FileSystems.get_scheme(path)
        if scheme == 'query':
            # path contains a sql query
            # strip off the scheme and just return the rest
            path = path[8:]
        elif scheme == 'bq':
            # path is a reference to a big query table
            # strip off the scheme and just return the table id in path
            path = path[5:]
        elif scheme is None:
            # could be a local file or a sql query
            if path[0] in ('.', '/'):
                scheme = 'file'     # local file
            else:
                scheme = 'query'

        return scheme, path

    def _source(self, path):
        scheme, path = self._parse_source_sink(path)
        if scheme == 'bq':
            return BigQuerySource(table=path)
        elif scheme == 'query':
            return BigQuerySource(query=path)
        else:
            return io.ReadFromText(
                file_pattern=path,
                coder=JSONCoder()
            )

    def _sink(self, path, schema):
        scheme, path = self._parse_source_sink(path)
        if scheme == 'bq':
            return Sink(
                table=path,
                write_disposition=self.options.sink_write_disposition,
                schema=schema
            )
        elif scheme == 'query':
            raise RuntimeError("Cannot use a query as a sink")
        else:
            return io.WriteToText(
                file_path_prefix=path,
                coder=JSONCoder()
            )

    def build(self, pipeline):
        messages = pipeline | "ReadFromSource" >> self._source(self.options.messages_source)

        if self.options.window_size:
            messages = messages | 'timestamp' >> beam.ParDo(AddTimestampDoFn())
            messages = messages | 'window' >> WindowInto(window.FixedWindows(self.options.window_size))

        segmented = (
            messages
            | "timestamp2datetime" >> beam.ParDo(Timestamp2DatetimeDoFn())
            | "ExtractMMSI" >> Map(lambda row: (row['mmsi'], row))
            | "GroupByMMSI" >> GroupByKey('mmsi')
            | "Segment" >> Segment()
        )
        messages = segmented[Segment.OUTPUT_TAG_MESSAGES]
        segments = segmented[Segment.OUTPUT_TAG_SEGMENTS]
        (
            messages
            | "datetime2timestamp" >> beam.ParDo(Datetime2TimestampDoFn())
            | "WriteToMessagesSink" >> self._sink(path=self.options.messages_sink,
                                                  schema=output_schema.build())
        )
        (
            segments
            | "WriteToSegmentsSink" >> self._sink(path=self.options.segments_sink,
                                                  schema=segment_schema.build())
        )
        return pipeline
