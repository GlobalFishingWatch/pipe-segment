import os

from pipeline.transforms.source import Source
from pipeline.transforms.segment import Segment
from pipeline.transforms.sink import Sink
from apache_beam import io
from apache_beam import Map
from apache_beam import GroupByKey
from apache_beam import FlatMap

from coders import JSONCoder


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

        (
            pipeline
            | "ReadFromSource" >> self._source()
            | "ExtractMMSI" >> Map(lambda row: (row['mmsi'], row))
            | "GroupByMMSI" >> GroupByKey('mmsi')
            | "Segment" >> Segment()
            | "Flatten" >> FlatMap(lambda(k,v): v)
            | "WriteToSink" >> self._sink()
        )

        return pipeline
