from google.cloud import bigquery
from pipe_segment import tools
from pipe_segment.pipelines.segment_vessel_daily.config import SegmentVesselDailyConfig
from pipe_segment.utils.bq_tools import BigQueryHelper, DatePartitionedTable, Schemas
from pipe_segment.utils.template_tools import format_query
from pipe_segment.version import __version__
from datetime import timedelta
from pipe_segment.schemas import get_schema_path
import logging

logger = logging.getLogger(__name__)
SCHEMA_PATH = get_schema_path("segment_vessel_daily.schema.json")
QUERY = "segment_vessel_daily.sql.j2"
PARTITION_FIELD = "day"


def description(config: SegmentVesselDailyConfig) -> str:
    return f"""
* Pipeline: segment_vessel_daily
* Version: pipe-segment:{__version__}
* Arguments {config}
"""


class SegmentVesselDailyPipeline:
    """ Segment vessel daily process"""
    def __init__(self, config: SegmentVesselDailyConfig):
        self.config = config
        self.bq_helper = BigQueryHelper(
            bq_client=bigquery.Client(project=config.project),
            labels=config.labels,
        )
        self.prepare_output_tables()

    def prepare_output_tables(self):
        # The end date is exclusive, but the query that clears the table includes its last day.
        last_day = self.config.end_date - timedelta(days=1)
        table = self.get_output_table()

        self.bq_helper.ensure_table_exists(table)
        self.bq_helper.run_query(query=table.clear_query(self.config.start_date, last_day))

    def get_output_table(self):
        return DatePartitionedTable(
            table_id=self.config.bq_out_segment_vessel_daily,
            description=description(self.config),
            schema=Schemas.load_json_schema(SCHEMA_PATH),
            partitioning_field=PARTITION_FIELD,
        )

    def run(self):
        # run the query and store the values in the partitioned field.
        logger.info("Formatting the query with the parameters.")
        table = self.get_output_table()
        for single_day in list(tools.list_of_days(self.config.start_date, self.config.end_date)):
            query = format_query(
                QUERY,
                date=single_day,
                window_days=self.config.window_days,
                single_ident_min_freq=self.config.single_ident_min_freq,
                most_common_min_freq=self.config.most_common_min_freq,
                spoofing_threshold=self.config.spoofing_threshold,
                segment_identity=self.config.bq_in_segment_identity_daily,
            )
            logger.info(f"Running the Segment Vessel Daily query for {single_day}.")
            self.bq_helper.run_query_into_table(query=query, table=table)
