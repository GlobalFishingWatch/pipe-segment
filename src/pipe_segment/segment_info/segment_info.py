from google.cloud import bigquery
from pipe_segment.pipelines.segment_info.config import SegmentInfoConfig
from pipe_segment.utils.bq_tools import BigQueryHelper, Schemas, SimpleTable
from pipe_segment.utils.template_tools import format_query
from pipe_segment.version import __version__
from pipe_segment.schemas import get_schema_path
import logging

logger = logging.getLogger(__name__)
SCHEMA_PATH = get_schema_path("segment_info.schema.json")
QUERY = "segment_info.sql.j2"
CLUSTERING_FIELDS = "seg_id"


def description(config: SegmentInfoConfig) -> str:
    return f"""
* Pipeline: segment_info
* Version: pipe-segment:{__version__}
* Arguments {config}
"""


class SegmentInfoPipeline:
    """ Segment info process"""
    def __init__(self, config: SegmentInfoConfig):
        self.config = config
        self.bq_helper = BigQueryHelper(
            bq_client=bigquery.Client(project=config.project),
            labels=config.labels,
        )
        self.prepare_output_tables()

    def prepare_output_tables(self):
        table = self.get_output_table()
        self.bq_helper.ensure_table_exists(table)
        self.bq_helper.run_query(query=table.clear_query())

    def get_output_table(self):
        return SimpleTable(
            table_id=self.config.bq_out_segment_info,
            description=description(self.config),
            schema=Schemas.load_json_schema(SCHEMA_PATH),
            clustering_field=CLUSTERING_FIELDS,
        )

    def run(self):
        # run the query and store the values in the partitioned field.
        logger.info("Formatting the query with the parameters.")
        query = format_query(
            QUERY,
            most_common_min_freq=self.config.most_common_min_freq,
            segment_identity_daily=self.config.bq_in_segment_identity_daily,
            segment_vessel_daily=self.config.bq_in_segment_vessel_daily,
        )
        logger.info("Running the Segment Info query.")
        self.bq_helper.run_query_into_table(query=query, table=self.get_output_table())
