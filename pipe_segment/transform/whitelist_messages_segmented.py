import apache_beam as beam
from pipe_segment.message_schema import message_output_schema as mos

fieldnames = list(map(lambda x: x['name'], mos['fields']))

class WhitelistFields(beam.PTransform):

    def whitelist(self, elem):
        return {field:elem[field] for field in fieldnames if field in elem}

    def expand(self, xs):
        return xs | beam.Map(self.whitelist)
