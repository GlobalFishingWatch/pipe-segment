import apache_beam as beam

from stdnum import imo as imo_validator

from shipdataprocess.normalize import normalize_callsign
from shipdataprocess.normalize import normalize_shipname

class NormalizeDoFn(beam.DoFn):
    @staticmethod
    def add_normalized_field(msg, fn, field):
        normalized = fn(msg.get(field))
        if normalized is not None:
            msg['n_'+field] = normalized

    def process(self, msg):
        self.add_normalized_field(msg, normalize_shipname, 'shipname')
        self.add_normalized_field(msg, normalize_callsign, 'callsign')

        imo = msg.get('imo')
        if imo_validator.is_valid(str(imo)):
            msg['n_imo'] = imo

        yield msg
