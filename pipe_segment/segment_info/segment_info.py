from google.cloud import bigquery
from pipe_segment.utils.bq_tools import BigQueryHelper, Schemas, SimpleTable
from pipe_segment.utils.template_tools import format_query
from pipe_segment.version import __version__
import logging

logger = logging.getLogger(__name__)
SCHEMA_PATH = "./assets/schemas/segment_info.schema.json"
QUERY = "segment_info.sql.j2"
CLUSTERING_FIELDS = "seg_id"


def description(options: str) -> str:
    return f"""
* Pipeline: segment_info
* Version: pipe-segment:{__version__}
* Arguments {options}
"""


class SegmentInfoPipeline:
    """ Segment info process"""
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
            clustering_field=CLUSTERING_FIELDS,
        )

    def run(self):
        # run the query and store the values in the partitioned field.
        logger.info("Formatting the query with the parameters.")
        query = format_query(
            QUERY,
            most_common_min_freq=self.options.most_common_min_freq,
            segment_identity_daily=self.options.source_segment_identity,
            segment_vessel_daily=self.options.source_segment_vessel,
        )
        logger.info("Running the Segment Info query.")
        self.bqtools.run_query(query=query, table=self.get_output_table())


def run(*args, **kwargs):
    return SegmentInfoPipeline.build(*args, **kwargs).run()
