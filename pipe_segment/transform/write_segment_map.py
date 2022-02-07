import apache_beam as beam
from datetime import date

schema = {
    "fields": [
        {
            "description": "A unique id for this segment",
            "mode": "REQUIRED",
            "name": "seg_id",
            "type": "STRING",
        },
        {
            "description": "A fragment id associated with this segment",
            "mode": "REQUIRED",
            "name": "frag_id",
            "type": "STRING",
        },
        {
            "description": "Date associated with the fragment",
            "mode": "REQUIRED",
            "name": "date",
            "type": "DATE",
        },
    ]
}


class WriteSegmentMap(beam.PTransform):
    def __init__(self, sink, project):
        self.sink = sink
        self.project = project

    def date_as_str(self, msg):
        msg["date"] = f"{msg['date']:%Y-%m-%d}"
        return msg

    def compute_table_for_event(self, msg):
        return f"{self.project}:{self.sink}{msg['date'].replace('_', '')}"

    def expand(self, pcoll):
        return (
            pcoll
            | beam.Map(self.date_as_str)
            | beam.io.WriteToBigQuery(
                self.compute_table_for_event,
                schema=schema,
                write_disposition="WRITE_TRUNCATE",
            )
        )
