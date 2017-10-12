import pytest
import posixpath as pp

from apache_beam.io.gcp.internal.clients import bigquery

from pipeline import schemas


@pytest.mark.filterwarnings('ignore:Using fallback coder:UserWarning')
@pytest.mark.filterwarnings('ignore:The compiler package is deprecated and removed in Python 3.x.:DeprecationWarning')
class TestSchema():
    def test_json_schema(self, test_data_dir):
        schema_file = pp.join(test_data_dir, 'messages-schema.json')
        with open(schema_file, 'r') as f:
            schema_string = f.read()
        schema = schemas.input.build(schema_string)
        assert isinstance(schema, bigquery.TableSchema)
