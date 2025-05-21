from copy import deepcopy


message_input_schema = [
    {
        "description": (
            "A unique id, we try using the spire raw id but sometimes it is not unique."
        ),
        "mode": "NULLABLE",
        "name": "msgid",
        "type": "STRING",
    },
    {
        "description": (
            "spire/orbcomm used on posterior steps when both spire"
            "and orbcomm are merge to know where they come from."
        ),
        "mode": "NULLABLE",
        "name": "source",
        "type": "STRING",
    },
    {
        "description": "The AIS messages, the format is [AIS.type]",
        "mode": "NULLABLE",
        "name": "type",
        "type": "STRING",
    },
    {
        "description": "The Specific Source Vessel ID, in this case the MMSI",
        "mode": "NULLABLE",
        "name": "ssvid",
        "type": "STRING",
    },
    {
        "description": "The timestap that indicates when the message was transmitted.",
        "mode": "NULLABLE",
        "name": "timestamp",
        "type": "TIMESTAMP",
    },
    {
        "description": "The longitude included in the spire message.",
        "mode": "NULLABLE",
        "name": "lon",
        "type": "FLOAT",
    },
    {
        "description": "The latitude included in the spire message.",
        "mode": "NULLABLE",
        "name": "lat",
        "type": "FLOAT",
    },
    {
        "description": "The speed in knots included in the spire json decoded message.",
        "mode": "NULLABLE",
        "name": "speed",
        "type": "FLOAT",
    },
    {
        "description": "The course included in the spire json decoded message.",
        "mode": "NULLABLE",
        "name": "course",
        "type": "FLOAT",
    },
    {
        "description": "The heading included in the spire json decoded message.",
        "mode": "NULLABLE",
        "name": "heading",
        "type": "FLOAT",
    },
    {
        "description": "The shipname included in the spire json decoded message.",
        "mode": "NULLABLE",
        "name": "shipname",
        "type": "STRING",
    },
    {
        "description": "The callsign included in the spire json decoded message.",
        "mode": "NULLABLE",
        "name": "callsign",
        "type": "STRING",
    },
    {
        "description": "The destination included in the spire json decoded message.",
        "mode": "NULLABLE",
        "name": "destination",
        "type": "STRING",
    },
    {
        "description": "The imo included in the spire json decoded message.",
        "mode": "NULLABLE",
        "name": "imo",
        "type": "STRING",
    },
    {
        "description": (
            "The shiptype included in the spire message using the shiptype.json match."
        ),
        "mode": "NULLABLE",
        "name": "shiptype",
        "type": "STRING",
    },
    {
        "description": (
            "terrestrial or satellite. "
            "Obtained through the collection_type in the spire json decoded message."
        ),
        "mode": "NULLABLE",
        "name": "receiver_type",
        "type": "STRING",
    },
{
    "description": "The source from spire json decoded message.",
    "mode": "NULLABLE",
    "name": "receiver",
    "type": "STRING",
},
{
    "description": "The length of the vessel from the SPIRE message.",
    "mode": "NULLABLE",
    "name": "length",
    "type": "FLOAT",
},
{
    "description": "The width of the vessel from the SPIRE message.",
    "mode": "NULLABLE",
    "name": "width",
    "type": "FLOAT",
},
{
    "description": "The navigational status.",
    "mode": "NULLABLE",
    "name": "status",
    "type": "INTEGER",
},
]



message_output_schema = deepcopy(message_input_schema)
message_output_schema.extend(
    [
        {"name": "seg_id", "type": "STRING", "mode": "NULLABLE"},
        {"name": "frag_id", "type": "STRING", "mode": "NULLABLE"},
    ]
)


fieldnames = list(map(lambda x: x['name'], message_output_schema))
