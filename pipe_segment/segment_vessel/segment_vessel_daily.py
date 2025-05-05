from pipe_segment.utils.bqtools import BigQueryTools
from pipe_segment.utils.template_tools import format_query
from pipe_segment.version import __version__
import logging
import typing

logger = logging.getLogger(__name__)
SCHEMA_PATH = "./assets/schemas/segment_vessel_daily.schema.json"
QUERY = "segment_vessel_daily.sql.j2"
PARTITION_FIELD = "day"
SOURCE_PARTITION_FIELD = "summary_timestamp"


def description(options: str) -> str:
    return f"""
* Pipeline: segment_vessel_daily
* Version: pipe-segment:{__version__}
* Arguments {options}
"""


def parse_labels(ll: typing.List[str]) -> typing.Dict:
    """Parses the labels from [k=v,k2=v2] -> {k1:v1,k2:v2}"""
    return dict(map(lambda x: (x.split("=")), ll))


class SegmentVesselDailyPipeline:
    """ Segment vessel daily process"""
    def __init__(self, options, extra_options):
        self.options = options
        self.bqtools = BigQueryTools(project=self.options.project)

        self.bqtools.remove_content(
            table=self.options.destination,
            date_range=self.options.date_range,
            labels=self.options.labels,
            partition_field=PARTITION_FIELD,
        )

    @classmethod
    def build(cls, options, extra_args):
        return cls(options, extra_args)

    def run(self):
        # run the query and store the values in the partitioned field.
        start, end = self.options.date_range.split(",")
        logger.info("Formatting the query with the parameters.")
        query = format_query(
            QUERY,
            date=end,
            window_days=self.options.window_days,
            single_ident_min_freq=self.options.single_ident_min_freq,
            most_common_min_freq=self.options.most_common_min_freq,
            spoofing_threshold=self.options.spoofing_threshold,
            segment_identity=self.options.source_segment_identity,
            segment_identity_partition_field=SOURCE_PARTITION_FIELD,
        )
        table = self.options.destination
        logger.debug(f"Query: {query}")
        logger.info("Rnuning the Segment Vessel Daily query.")
        self.bqtools.run_query(
            query,
            dest_table=table,
            write_disposition="WRITE_APPEND",
            partition_field=PARTITION_FIELD,
            clustering_fields=[PARTITION_FIELD],
            labels=parse_labels(self.options.labels),
        )
        self.bqtools.update_table_schema(table, SCHEMA_PATH)
        # updates the label and description
        self.bqtools.update_description(table, description(self.options))
        self.bqtools.update_labels(table, self.options.labels)


def run(*args, **kwargs):
    return SegmentVesselDailyPipeline.build(*args, **kwargs).run()
