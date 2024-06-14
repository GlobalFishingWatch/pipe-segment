import apache_beam as beam
from pipe_segment import message_schema


class WhitelistFields(beam.PTransform):
    def __init__(self, fieldnames=None):
        self._fieldnames = fieldnames

        if self._fieldnames is None:
            self._fieldnames = message_schema.fieldnames

    def whitelist(self, elem):
        return {field: elem[field] for field in self._fieldnames if field in elem}

    def expand(self, xs):
        return xs | beam.Map(self.whitelist)
