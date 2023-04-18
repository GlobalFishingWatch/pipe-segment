from datetime import timedelta
from importlib import resources
from jinja2 import Template

import apache_beam as beam
import apache_beam.io.gcp.bigquery
import logging, functools
from apache_beam import PTransform, io
from google.api_core.exceptions import (NotFound, BadRequest)
from google.cloud import bigquery


def remove_satellite_offsets_content(destination_table:str, date_range:str, labels_list: list, project:str):
    """
    The satellite offset table is now a partitioned table. It removes the content of the satellite offeset table for the period of date range to let the DF job generate the new data.
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
    date_from, date_to = list(map(lambda x:x.strip(), date_range.split(',')))
    labels = functools.reduce(lambda x,y: dict(x,**{y.split('=')[0]: y.split('=')[1]}), labels_list, dict())

    try:
        table = client.get_table(destination_table_ref) #API request
        logging.info(f'Ensures the table [{table}] exists.')
        #deletes the content
        query_job = client.query(
            f"""
               DELETE FROM `{ destination_table }`
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
        logging.info(f'Date range [{date_from},{date_to}] cleaned in table {destination_table}: {result}')

    except NotFound as nferr:
        logging.warn(f'Table {destination_table} NOT FOUND. We can go on.')

    except BadRequest as err:
        logging.error(f'Bad request received {err}.')

def make_schema():
    schema = {"fields": []}

    def add_field(name, field_type, mode="REQUIRED"):
        schema["fields"].append(
            dict(
                name=name,
                type=field_type,
                mode=mode,
            )
        )

    add_field("hour", "TIMESTAMP")
    add_field("receiver", "STRING")
    add_field("dt", "FLOAT", "NULLABLE")
    add_field("pings", "INTEGER", "NULLABLE")
    add_field("avg_distance_from_sat_km", "FLOAT", "NULLABLE")
    add_field("med_dist_from_sat_km", "FLOAT", "NULLABLE")

    return schema


class SatelliteOffsets(PTransform):
    """Generate satellite offset times based on Spire data.

    Example
    -------

        offsets = pipesline | SatelliteOffsets(start_date, end_date)
    """

    schema = make_schema()

    def __init__(self, source_table, norad_to_receiver_tbl, sat_positions_tbl, 
                start_date, end_date):
        self.source_table = source_table
        self.norad_to_receiver_tbl = norad_to_receiver_tbl
        self.sat_positions_tbl = sat_positions_tbl
        self.start_date = start_date
        self.end_date = end_date

    def expand(self, xs):
        return [
            xs | "SatOffsets{}".format(ndx) >> src
            for (ndx, src) in enumerate(self._sat_offset_iter())
        ] | "MergeSatOffsets" >> beam.Flatten()

    def _sat_offset_iter(self):
        with resources.path('pipe_segment.transform.assets', 'satellite_offsets.sql.j2') as template_filepath:
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
            yield io.Read(
                io.gcp.bigquery.BigQuerySource(query=query, use_standard_sql=True)
            )

    def _get_query_windows(self):
        start_date, end_date = self.start_date, self.end_date
        start_window = start_date
        while start_window <= end_date:
            end_window = min(start_window + timedelta(days=800), end_date)
            yield start_window, end_window
            start_window = end_window + timedelta(days=1)
