from gpsdio_segment.msg_processor import Destination, Identity


def make_schema():
    schema = {"fields": []}

    def add_field(name, field_type, mode="REQUIRED", index=None):
        if index is None:
            index = len(schema["fields"])
        schema["fields"].insert(
            index,
            dict(
                name=name,
                type=field_type,
                mode=mode,
            ),
        )

    add_field("frag_id", "STRING")
    add_field("seg_id", "STRING", index=0)
    add_field("ssvid", "STRING")
    add_field("timestamp", "TIMESTAMP")

    def add_ident_field(name, value_type):
        field = dict(
            name=name,
            type="RECORD",
            mode="REPEATED",
            fields=[dict(name="count", type="INTEGER", mode="NULLABLE")],
        )
        for fld_name in value_type._fields:
            field["fields"].append(dict(name=fld_name, type="STRING", mode="NULLABLE"))
        schema["fields"].append(field)

    add_field("daily_msg_count", "INTEGER")
    add_ident_field("daily_identities", Identity)
    add_ident_field("daily_destinations", Destination)
    add_field("first_timestamp", "TIMESTAMP")
    add_field("cumulative_msg_count", "INTEGER")
    add_ident_field("cumulative_identities", Identity)
    add_ident_field("cumulative_destinations", Destination)

    return schema


segment_schema = make_schema()
