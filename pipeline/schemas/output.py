from apache_beam.io.gcp.internal.clients import bigquery

from pipeline.schemas import input

def build():
    schema = input.build()

    field = bigquery.TableFieldSchema()
    field.name = "seg_id"
    field.type = "STRING"
    field.mode="NULLABLE"
    schema.fields.append(field)

    return schema
