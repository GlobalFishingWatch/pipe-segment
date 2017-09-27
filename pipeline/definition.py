from pipeline.transforms.source import Source
from pipeline.transforms.identity import Identity
from pipeline.transforms.sink import Sink
from apache_beam import io
from apache_beam import Map
from apache_beam import GroupByKey

class PipelineDefinition():
    def __init__(self, options):
        self.options = options

    def build(self, pipeline):
        if self.options.local:
            sink = io.WriteToText('output/events')
        elif self.options.remote:
            sink = Sink(
                table=self.options.sink,
                write_disposition=self.options.sink_write_disposition,
            )

        (
            pipeline
            | "ReadFromSource" >> Source(self.options.source)
            | "ExtractMMSI" >> Map(lambda row: (row['mmsi'], row))
            | "GroupByMMSI" >> GroupByKey('mmsi')
            | "DoNothing" >> Identity()
            | "WriteToSink" >> sink
        )

        return pipeline
