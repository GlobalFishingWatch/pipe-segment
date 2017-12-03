import pytest
import posixpath as pp
from datetime import datetime
from pytz import UTC

from apache_beam.io.gcp.internal.clients import bigquery
from pipe_tools.timestamp import timestampFromDatetime

from pipe_template.io.bigquery import QueryBuilder


@pytest.mark.filterwarnings('ignore:Using fallback coder:UserWarning')
@pytest.mark.filterwarnings('ignore:The compiler package is deprecated and removed in Python 3.x.:DeprecationWarning')
class TestBigquery():

    @pytest.mark.slow
    def test_query_builder(self):
        # table='bigquery-public-data:san_francisco.bikeshare_trips'

        # TODO: need a real table that is public and date sharded
        table='world-fishing-827:scratch_paul.shard_test_a_'

        first_date_ts = timestampFromDatetime(datetime(2017,1,1, tzinfo=UTC))
        last_date_ts = timestampFromDatetime(datetime(2017,1,2, tzinfo=UTC))
        builder = QueryBuilder(table=table, first_date_ts=first_date_ts, last_date_ts=last_date_ts)
        assert builder.filter_table_schema() == builder.table_schema

        fields = [f.name for f in builder.table_schema.fields]
        filtered_schema = builder.filter_table_schema(include_fields=sorted(fields)[:len(fields)/2])
        assert len(filtered_schema.fields) == len(fields)/2

        # TODO: need a way to test the query
        sql =  builder.build_query(include_fields=fields)
