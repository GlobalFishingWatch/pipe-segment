from apache_beam.io.gcp.internal.clients import bigquery

from pipeline.schemas import input

def build():
    schema = bigquery.TableSchema()

    field = bigquery.TableFieldSchema()
    field.name = "seg_id"
    field.type = "STRING"
    field.mode="REQUIRED"
    schema.fields.append(field)

    field = bigquery.TableFieldSchema()
    field.name = "message_count"
    field.type = "INTEGER"
    field.mode="REQUIRED"
    schema.fields.append(field)

    field = bigquery.TableFieldSchema()
    field.name = "mmsi"
    field.type = "INTEGER"
    field.mode="REQUIRED"
    schema.fields.append(field)

    return schema
