from apache_beam import PTransform
from apache_beam import io

class BigQuerySource(PTransform):
    def __init__(self, query=None, table=None):
        self.query = query
        self.table = table

    def expand(self, xs):
        return (
            xs
            | io.Read(io.gcp.bigquery.BigQuerySource(query=self.query, table=self.table))
        )
