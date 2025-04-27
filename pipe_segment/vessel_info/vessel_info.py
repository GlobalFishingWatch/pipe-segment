from pipe_segment.utils.bqtools import BigQueryTools
from pipe_segment.utils.template_tools import format_query
from pipe_segment.version import __version__
from pipe_segment.tools import as_timestamp
import logging
import typing

logger = logging.getLogger(__name__)
SCHEMA_PATH = "./assets/schemas/vessel_info.schema.json"
QUERY = "vessel_info.sql.j2"


def description(options: str) -> str:
    return f"""
* Pipeline: vessel_info
* Version: pipe-segment:{__version__}
* Arguments {options}
"""


def parse_labels(ll: typing.List[str]) -> typing.Dict:
    """Parses the labels from [k1=v1,k2=v2] -> {k1:v1,k2:v2}"""
    return dict(map(lambda x: (x.split("=")), ll))


class VesselInfoPipeline:
    """ Vessel info process"""
    def __init__(self, options, extra_options):
        self.options = options
        self.bqtools = BigQueryTools(project=self.options.project)

    @classmethod
    def build(cls, options, extra_args):
        return cls(options, extra_args)

    def run(self):
        # run the query and store the values in the partitioned field.
        logger.info("Formatting the query with the parameters.")
        query = format_query(
            QUERY,
            most_common_min_freq=self.options.most_common_min_freq,
            segment_identity_daily=self.options.source_segment_identity,
            segment_vessel_daily=self.options.source_segment_vessel,
        )
        table = self.options.destination
        logger.debug(f"Query: {query}")
        logger.info("Running the Vessel Info query.")
        self.bqtools.run_query(
            query,
            dest_table=table,
            write_disposition="WRITE_TRUNCATE",
            labels=parse_labels(self.options.labels),
        )
        self.bqtools.update_table_schema(table, SCHEMA_PATH)
        # updates the label and description
        self.bqtools.update_description(table, description(self.options))
        self.bqtools.update_labels(table, self.options.labels)


def run(*args, **kwargs):
    return VesselInfoPipeline.build(*args, **kwargs).run()
