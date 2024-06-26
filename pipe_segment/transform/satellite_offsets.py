from datetime import timedelta
from importlib import resources
from jinja2 import Template

import apache_beam as beam
import logging
import functools
from apache_beam import PTransform, io
from google.api_core.exceptions import (NotFound, BadRequest)
from google.cloud import bigquery
from pipe_segment.version import __version__


def remove_satellite_offsets_content(
        destination_table: str,
        date_range: str,
        labels_list: list,
        project: str):
    """
    The satellite offset table is now a partitioned table.
    It removes the content of the satellite offeset table for the period of date range
    to let the DF job generate the new data.
    :param destination_table: The destination table from where to delete the content.
    :type destination_table: str.
    :param date_range: The date start and end to delete the content.
    :type date_range: str.
    :param labels_list: The labels to be assigned when interact with BQ.
    :type labels_list: list.
    :param project: The google cloud project name.
    :type project: str.
    """
    client = bigquery.Client(project)
    destination_table_ds, destination_table_tb = destination_table.split('.')
    destination_dataset_ref = bigquery.DatasetReference(client.project, destination_table_ds)
    destination_table_ref = destination_dataset_ref.table(destination_table_tb)
    date_from, date_to = list(map(lambda x: x.strip(), date_range.split(',')))
    labels = functools.reduce(lambda x, y: dict(
        x, **{y.split('=')[0]: y.split('=')[1]}), labels_list, dict())

    try:
        table = client.get_table(destination_table_ref)  # API request
        logging.info(f'Ensures the table [{table}] exists.')
        # deletes the content
        query_job = client.query(
            f"""
               DELETE FROM `{destination_table}`
               WHERE date(hour) BETWEEN '{date_from}' AND '{date_to}'
            """,
            bigquery.QueryJobConfig(
                use_query_cache=False,
                use_legacy_sql=False,
                labels=labels,
            )
        )
        logging.info(f'Delete Job {query_job.job_id} is currently in state {query_job.state}')
        result = query_job.result()
        logging.info(
            f'Date range [{date_from},{date_to}] cleaned in table {destination_table}: {result}')

    except NotFound:
        logging.warn(f'Table {destination_table} NOT FOUND. We can go on.')

    except BadRequest as err:
        logging.error(f'Bad request received {err}.')


def make_schema():
    schema = {"fields": []}

    def add_field(name, field_type, mode="REQUIRED", description=""):
        schema["fields"].append(
            dict(
                name=name,
                type=field_type,
                mode=mode,
                description=description,
            )
        )

    add_field(
        "hour",
        "TIMESTAMP",
        description=(
            "Timestamp with hour resolution. "
            "This row applies starting from `hour` till `hour` plus sixty minutes.")
    )
    add_field("receiver", "STRING", description="The name of the satellite.")
    add_field(
        "dt",
        "FLOAT",
        "NULLABLE",
        description=(
            "How much the satellite clock is offset (in seconds)"
            "from the consensus (median) of all satellite clocks.")
        )
    add_field(
        "pings",
        "INTEGER",
        "NULLABLE",
        description=(
            "Number of pings used in computing the clock offset. "
            "Lower values mean the result is likely to be less reliable.")
        )
    add_field(
        "avg_distance_from_sat_km",
        "FLOAT",
        "NULLABLE",
        description=(
            "Average distance of vessels from the satellite during a given hour. "
            "Anomalously large values are another way to infer that the satellite clocks are off, "
            "however this has been superseded by the dt field.")
        )
    add_field(
        "med_dist_from_sat_km",
        "FLOAT",
        "NULLABLE",
        description=(
            "Median distance of vessels from the satellite during a given hour. "
            "Anomalously large values are another way to infer that the satellite clocks are off, "
            "however this has been superseded by the dt field.")
        )
    return schema


class SatelliteOffsets(PTransform):
    """Generate satellite offset times based on Spire data.

    Example
    -------

        offsets = pipesline | SatelliteOffsets(start_date, end_date)
    """

    def __init__(self, source_table, norad_to_receiver_tbl, sat_positions_tbl,
                 start_date, end_date):
        self.source_table = source_table
        self.norad_to_receiver_tbl = norad_to_receiver_tbl
        self.sat_positions_tbl = sat_positions_tbl
        self.start_date = start_date
        self.end_date = end_date

    def expand(self, xs):
        return [
            xs | f"SatOffsets{i}" >> src for (i, src) in enumerate(self._sat_offset_iter())
        ] | "MergeSatOffsets" >> beam.Flatten()

    def _sat_offset_iter(self):
        with resources.path(
            'pipe_segment.transform.assets', 'satellite_offsets.sql.j2'
        ) as template_filepath:
            with open(template_filepath) as f:
                template = Template(f.read())

        for start_window, end_window in self._get_query_windows():
            query = template.render(
                source=self.source_table,
                norad_to_receiver_tbl=self.norad_to_receiver_tbl,
                sat_positions_tbl=self.sat_positions_tbl,
                start_window=start_window,
                end_window=end_window
            )
            yield io.ReadFromBigQuery(query=query, use_standard_sql=True)

    def _get_query_windows(self):
        start_date, end_date = self.start_date, self.end_date
        start_window = start_date
        while start_window <= end_date:
            end_window = min(start_window + timedelta(days=800), end_date)
            yield start_window, end_window
            start_window = end_window + timedelta(days=1)


def list_to_dict(labels):
    return {x.split('=')[0]: x.split('=')[1] for x in labels}


class SatelliteOffsetsWrite(PTransform):

    def __init__(self, options, cloud_opts):
        self.bqclient = bigquery.Client(cloud_opts.project)
        self.source_table = options.sat_source
        self.source_norad = options.norad_to_receiver_tbl
        self.source_sat_positions = options.sat_positions_tbl
        _, self.end_date = options.date_range.split(',')
        self.labels = list_to_dict(cloud_opts.labels)

        self.dest_table = options.sat_offset_dest
        dataset_id, table_name = self.dest_table.split('.')
        self.table_ref = bigquery.DatasetReference(
            cloud_opts.project, dataset_id).table(table_name)

        self.schema = make_schema()
        self.ver = __version__

    def expand(self, xs):
        return xs | "WriteSatOffsets" >> io.WriteToBigQuery(
            self.dest_table,
            schema=self.schema,
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
            additional_bq_parameters={
                'timePartitioning': {
                    'type': 'MONTH',
                    'field': 'hour',
                    'requirePartitionFilter': False
                }, 'clustering': {
                    'fields': ['hour']
                }
            }
        )

    def update_table_description(self):
        table = self.bqclient.get_table(self.table_ref)  # API request
        table.description = f"""
    Created by the pipe-segment: {self.ver}
    * It identifies, at the hourly level,
    * how much time a given satellite's clock differs
    * from the median of all the other satellite's clocks.
    * https://github.com/GlobalFishingWatch/pipe-segment
    * Source Satellite: {self.source_table}
    * Source Norad: {self.source_norad}
    * Source Satellite Positions: {self.source_sat_positions}
    * Date: {self.end_date}
        """
        table_updated = self.bqclient.update_table(table, ["description"])  # API request
        assert table_updated.description == table.description
        logging.info(f"Update descriptions to output table <{self.dest_table}>")

    def update_labels(self):
        table = self.bqclient.get_table(self.table_ref)  # API request
        table.labels = self.labels
        self.bqclient.update_table(table, ["labels"])  # API request
        logging.info(f"Update labels to output table <{self.dest_table}>")
