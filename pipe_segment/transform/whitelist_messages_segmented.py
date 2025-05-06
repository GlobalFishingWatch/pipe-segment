import apache_beam as beam
from pipe_segment.schemas import message_schema


class WhitelistFields(beam.PTransform):
    def __init__(self, fieldnames=message_schema.fieldnames):
        self._fieldnames = fieldnames

    def whitelist(self, elem):
        return {field: elem[field] for field in self._fieldnames if field in elem}

    def expand(self, xs):
        return xs | beam.Map(self.whitelist)
