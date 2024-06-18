from dataclasses import dataclass
from datetime import datetime

import apache_beam as beam
from google.cloud import bigquery
from google.api_core.exceptions import NotFound, BadRequest


from pipe_segment.transform.satellite_offsets import SatelliteOffsets, SatelliteOffsetsWrite
from pipe_segment.transform.satellite_offsets import remove_satellite_offsets_content

from apache_beam.testing.test_pipeline import TestPipeline


class ReadFromBigQueryMock(beam.io.ReadFromBigQuery):
    def expand(self, pcoll):
        return pcoll | beam.Create(["dummy"])


class WriteToBigQueryMock(beam.io.WriteToBigQuery):
    def __init__(self, table, *args, **kwargs):
        self._table = table
        super().__init__(table, *args, **kwargs)

    def expand(self, pcoll):
        return pcoll | beam.Map(self._table)


class RowMock:
    min_suffix = "20240301"


@dataclass
class ResultMock:
    job_id = '1234'
    state = 0
    exception: Exception = None
    exception_message: str = ""

    def result(self):
        if self.exception:
            raise self.exception(self.exception_message)

        return [RowMock()]


@dataclass
class TableMock:
    description = "description"
    labels = []


@dataclass
class BigQueryClientMock:
    project: str = 'gfw'
    notfound: bool = False
    badrequest: bool = False

    def query(self, query, config=None):
        if self.badrequest:
            return ResultMock(exception=BadRequest)

        if self.notfound:
            return ResultMock(exception=NotFound)

        return ResultMock()

    def get_table(self, table):
        return TableMock()

    def update_table(self, table, fields):
        return table


class OptionsMock:
    date_range = '2024-01-01, 2024-01-01'
    in_normalized_sat_offset_messages_table = "project.dummy_table"
    in_norad_to_receiver_table = "project.dummy_table"
    in_sat_positions_table = "project.dummy_table"
    out_sat_offsets_table = "project.dummy_table"


class CloudOptionsMock:
    project = "mock"
    labels = []


def test_satellite_offsets(monkeypatch):
    # TODO: replace this monkey patch when design allows for more easy testing.
    monkeypatch.setattr(beam.io, "ReadFromBigQuery", ReadFromBigQueryMock)
    monkeypatch.setattr(beam.io, "WriteToBigQuery", ReadFromBigQueryMock)

    remove_sat_offsets_config = dict(
        destination_table=OptionsMock.out_sat_offsets_table,
        date_range=OptionsMock.date_range,
        project=CloudOptionsMock.project,
        labels_list=CloudOptionsMock.labels
    )

    monkeypatch.setattr(bigquery, "Client", lambda *x, **y: BigQueryClientMock(notfound=True))
    remove_satellite_offsets_content(**remove_sat_offsets_config)

    monkeypatch.setattr(bigquery, "Client", lambda *x, **y: BigQueryClientMock(badrequest=True))
    remove_satellite_offsets_content(**remove_sat_offsets_config)

    monkeypatch.setattr(bigquery, "Client", BigQueryClientMock)
    remove_satellite_offsets_content(**remove_sat_offsets_config)

    dummy_table = ""
    start_date = datetime(2024, 1, 1)
    end_date = datetime(2024, 1, 1)

    query_op = SatelliteOffsets(dummy_table, dummy_table, dummy_table, start_date, end_date)
    write_op = SatelliteOffsetsWrite(OptionsMock(), CloudOptionsMock())

    # messages = []
    with TestPipeline() as p:

        p | query_op
        p | write_op

        write_op.update_table_description()
        write_op.update_labels()
