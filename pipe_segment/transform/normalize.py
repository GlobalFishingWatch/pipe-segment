import apache_beam as beam

from stdnum import imo as imo_validator

# TODO: reinstate when we've got everything working
# from shipdataprocess.normalize import normalize_callsign
# from shipdataprocess.normalize import normalize_shipname


class NormalizeDoFn(beam.DoFn):
    @staticmethod
    def add_normalized_field(msg, fn, field):
        normalized = fn(msg.get(field))
        if normalized is not None:
            msg["n_" + field] = normalized

    def process(self, msg):
        n_shipname = None  # normalize_shipname(msg.get("shipname"))
        if n_shipname:
            msg["n_shipname"] = n_shipname

        n_callsign = None  # normalize_callsign(msg.get("callsign"))
        if n_callsign:
            msg["n_callsign"] = n_callsign

        imo = msg.get("imo")
        if imo_validator.is_valid(str(imo)):
            msg["n_imo"] = imo

        yield msg
