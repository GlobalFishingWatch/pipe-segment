import apache_beam as beam

from stdnum import imo as imo_validator

from shipdataprocess.normalize import normalize_callsign
from shipdataprocess.normalize import normalize_shipname_parts

class NormalizeDoFn(beam.DoFn):
    @staticmethod
    def add_normalized_field(msg, fn, field):
        normalized = fn(msg.get(field))
        if normalized is not None:
            msg['n_'+field] = normalized

    def process(self, msg):
        n_shipname = normalize_shipname_parts(msg.get('shipname'))['basename']
        if n_shipname:
            msg['n_shipname'] = n_shipname

        n_callsign = normalize_callsign(msg.get('callsign'))
        if n_callsign:
            msg['n_callsign'] = n_callsign

        imo = msg.get('imo')
        if imo_validator.is_valid(str(imo)):
            msg['n_imo'] = imo

        yield msg
