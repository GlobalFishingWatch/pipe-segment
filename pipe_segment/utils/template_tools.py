from jinja2 import (
    Environment,
    FileSystemLoader,
    StrictUndefined,
)


def format_query(template_file: str, **params) -> str:
    """
    Format a jinja2 templated query with the given params.

    You may have extra params which are not used by the template, but all params
    required by the template must be provided.
    :param template_file: The path to the template file.
    :param params: The dictionary of params to replace in the template.
    """
    jinja2_env = Environment(
        loader=FileSystemLoader(["./assets/queries/"]), undefined=StrictUndefined
    )
    sql_template = jinja2_env.get_template(template_file)
    formatted_template = sql_template.render(params)
    return formatted_template
