from apache_beam.io.gcp.internal.clients import bigquery

from pipe_template.schemas import input

def build(input_schema):
    schema = input_schema

    field = bigquery.TableFieldSchema()
    field.name = "seg_id"
    field.type = "STRING"
    field.mode="NULLABLE"
    schema.fields.append(field)

    return schema
