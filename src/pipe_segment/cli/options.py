"""Options shared by all the commands of the CLI."""

from gfw.common.cli import Option


HELP_PROJECT = "GCP project that runs the queries and the jobs, and is billed for them."


def project_option() -> Option:
    """Returns the required ``--project`` option.

    Returns:
        The option.
    """
    return Option("--project", type=str, required=True, help=HELP_PROJECT)
