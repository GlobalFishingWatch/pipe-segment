from datetime import datetime

import apache_beam as beam

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
    ]
}


class WriteSegmentMap(beam.PTransform):
    def __init__(self, sink, project, start_date, end_date, temp_location):
        self.sink = sink
        self.project = project
        self.temp_location = temp_location

    # def compute_table_for_event(self, event):
    #     stamp = datetime.fromtimestamp(event["timestamp"])
    #     return f"{self.project}:{self.sink.get()}{stamp:%Y%m%d}"

    def expand(self, pcoll):
        return pcoll | beam.io.WriteToBigQuery(
            self.sink,
            # self.compute_table_for_event,
            schema=schema,
            write_disposition="WRITE_TRUNCATE",
        )
