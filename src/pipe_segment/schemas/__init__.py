from importlib import resources

SCHEMAS_FOLDER = resources.files('pipe_segment.assets.schemas')


def get_schema_path(filename: str):
    return str(SCHEMAS_FOLDER / filename)
