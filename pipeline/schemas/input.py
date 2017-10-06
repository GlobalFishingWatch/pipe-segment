from apache_beam import io


def build(schema_string):

    return io.gcp.bigquery.parse_table_schema_from_json(schema_string)

    # schema = bigquery.TableSchema()
    #
    # field = bigquery.TableFieldSchema()
    # field.name = "mmsi"
    # field.type = "INTEGER"
    # field.mode="REQUIRED"
    # schema.fields.append(field)
    #
    # field = bigquery.TableFieldSchema()
    # field.name = "timestamp"
    # field.type = "TIMESTAMP"
    # field.mode="REQUIRED"
    # schema.fields.append(field)
    #
    # field = bigquery.TableFieldSchema()
    # field.name = "lat"
    # field.type = "FLOAT"
    # field.mode="NULLABLE"
    # schema.fields.append(field)
    #
    # field = bigquery.TableFieldSchema()
    # field.name = "lon"
    # field.type = "FLOAT"
    # field.mode="NULLABLE"
    # schema.fields.append(field)
    #
    # field = bigquery.TableFieldSchema()
    # field.name = "speed"
    # field.type = "FLOAT"
    # field.mode="NULLABLE"
    # schema.fields.append(field)
    #
    # field = bigquery.TableFieldSchema()
    # field.name = "tagblock_station"
    # field.type = "STRING"
    # field.mode="NULLABLE"
    # schema.fields.append(field)
    #
    # return schema
