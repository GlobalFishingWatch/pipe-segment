from apache_beam.io.gcp.internal.clients import bigquery

from pipeline.transforms.segment import Segment


FIELD_TYPES = {
    'timestamp': 'TIMESTAMP',
    'lat': 'FLOAT',
    'lat': 'FLOAT',
}

STAT_TYPES = {
    'most_common': 'STRING',
    'most_common_count': 'INTEGER',
    'count': 'INTEGER'
}


def build(stats_fields=Segment.DEFAULT_STATS_FIELDS):
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

    for field_name, stats in stats_fields:
        for stat_name in stats:
            field = bigquery.TableFieldSchema()
            field.name = Segment.stat_output_field_name(field_name, stat_name)
            field.type = STAT_TYPES.get(stat_name) or FIELD_TYPES.get(field_name, 'STRING')
            field.mode="NULLABLE"
            schema.fields.append(field)

    return schema
