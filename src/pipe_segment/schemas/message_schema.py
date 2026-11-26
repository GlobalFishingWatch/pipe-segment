"""Field names of the segmented messages table."""

import json

from importlib import resources


with resources.files("pipe_segment.assets.schemas").joinpath(
    "messages_segmented.schema.json"
).open() as f:
    fieldnames = [field["name"] for field in json.load(f)]
