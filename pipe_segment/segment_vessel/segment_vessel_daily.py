from google.cloud import bigquery
from pipe_segment.utils.bq_tools import BigQueryHelper, DatePartitionedTable, Schemas
from pipe_segment.utils.template_tools import format_query
from pipe_segment.version import __version__
import logging

logger = logging.getLogger(__name__)
SCHEMA_PATH = "./assets/schemas/segment_vessel_daily.schema.json"
QUERY = "segment_vessel_daily.sql.j2"
PARTITION_FIELD = "day"


def description(options: str) -> str:
    return f"""
* Pipeline: segment_vessel_daily
* Version: pipe-segment:{__version__}
* Arguments {options}
"""


class SegmentVesselDailyPipeline:
    """ Segment vessel daily process"""
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
        start_date, end_date = self.options.date_range.split(",")
        table = self.get_output_table()

        self.bq_helper.ensure_table_exists(table)
        self.bq_helper.run_query(query=table.clear_query(start_date, end_date))

    def get_output_table(self):
        return DatePartitionedTable(
            table_id=self.options.destination,
            description=description(self.options),
            schema=Schemas.load_json_schema(SCHEMA_PATH),
            partitioning_field=PARTITION_FIELD,
        )

    def run(self):
        # run the query and store the values in the partitioned field.
        _, end = self.options.date_range.split(",")
        logger.info("Formatting the query with the parameters.")
        query = format_query(
            QUERY,
            date=end,
            window_days=self.options.window_days,
            single_ident_min_freq=self.options.single_ident_min_freq,
            most_common_min_freq=self.options.most_common_min_freq,
            spoofing_threshold=self.options.spoofing_threshold,
            segment_identity=self.options.source_segment_identity,
        )
        logger.info("Running the Segment Vessel Daily query.")
        table = self.get_output_table()
        self.bq_helper.run_query_into_table(query=query, table=table)


def run(*args, **kwargs):
    return SegmentVesselDailyPipeline.build(*args, **kwargs).run()
