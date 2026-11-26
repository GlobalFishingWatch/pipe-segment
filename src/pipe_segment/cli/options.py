"""Options shared by all the commands of the CLI."""

from gfw.common.cli import Option


HELP_PROJECT = "GCP project that runs the queries and the jobs, and is billed for them."
HELP_MOCK_BQ_CLIENTS = (
    "If True, uses mocked BigQuery clients: nothing is read from or written to BigQuery. "
    "It is meant for tests."
)


def project_option() -> Option:
    """Returns the required ``--project`` option.

    Returns:
        The option.
    """
    return Option("--project", type=str, required=True, help=HELP_PROJECT)


def mock_bq_clients_option() -> Option:
    """Returns the ``--mock-bq-clients`` option.

    Returns:
        The option.
    """
    return Option("--mock-bq-clients", type=bool, help=HELP_MOCK_BQ_CLIENTS)
