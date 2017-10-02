import os

import apache_beam as beam
from apache_beam import io
from apache_beam import Map
from apache_beam import GroupByKey
from apache_beam import FlatMap
from apache_beam import WindowInto
from apache_beam.transforms import window

from pipeline.transforms.source import Source
from pipeline.transforms.segment import Segment
from pipeline.transforms.sink import Sink



from coders import JSONCoder

class AddTimestampDoFn(beam.DoFn):

  def process(self, msg):
    # Wrap and emit the current entry and new timestamp in a
    # TimestampedValue.
    yield window.TimestampedValue(msg, msg['timestamp'])


class PipelineDefinition():
    def __init__(self, options):
        self.options = options

    def _source(self):
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

    def _sink(self):
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

    def build(self, pipeline):

        items = pipeline | "ReadFromSource" >> self._source()

        if self.options.window_size:
            items = items | 'timestamp' >> beam.ParDo(AddTimestampDoFn())
            items = items | 'window' >> WindowInto(window.FixedWindows(self.options.window_size))

        (
            items | "ExtractMMSI" >> Map(lambda row: (row['mmsi'], row))
            | "GroupByMMSI" >> GroupByKey('mmsi')
            | "Segment" >> Segment()
            | "Flatten" >> FlatMap(lambda(k,v): v)
            | "WriteToSink" >> self._sink()
        )

        return pipeline
