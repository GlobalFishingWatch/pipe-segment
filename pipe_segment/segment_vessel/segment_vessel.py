from google.cloud import bigquery
from pipe_segment.utils.bq_tools import BigQueryHelper, Schemas, SimpleTable
from pipe_segment.utils.template_tools import format_query
from pipe_segment.version import __version__
import logging

logger = logging.getLogger(__name__)
SCHEMA_PATH = "./assets/schemas/segment_vessel.schema.json"
QUERY = "segment_vessel.sql.j2"


def description(options: str) -> str:
    return f"""
* Pipeline: segment_vessel
* Version: pipe-segment:{__version__}
* Arguments {options}
"""


class SegmentVesselPipeline:
    """ Segment vessel process"""
    def __init__(self, options, extra_options):
        self.options = options
        self.bq_helper = BigQueryHelper(
            bq_client=bigquery.Client(project=options.project),
            labels=options.labels,
        )
        self.prepare_output_tables()

    @classmethod
    def build(cls, options, extra_args):
        return cls(options, extra_args)

    def prepare_output_tables(self):
        table = self.get_output_table()
        self.bq_helper.ensure_table_exists(table)
        self.bq_helper.run_query(query=table.clear_query())

    def get_output_table(self):
        return SimpleTable(
            table_id=self.options.destination,
            description=description(self.options),
            schema=Schemas.load_json_schema(SCHEMA_PATH),
        )

    def run(self):
        # run the query and store the values in the partitioned field.
        logger.info("Formatting the query with the parameters.")
        query = format_query(
            QUERY,
            segment_vessel_daily=self.options.source_segment_vessel_daily,
        )
        logger.info("Running the Segment vessel query.")
        self.bq_helper.run_query_into_table(query=query, table=self.get_output_table())


def run(*args, **kwargs):
    return SegmentVesselPipeline.build(*args, **kwargs).run()
