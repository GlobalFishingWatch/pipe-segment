"""Tests for the CLI as a whole."""

import os

import pytest

from pytest_mock import MockerFixture

from pipe_segment.cli import main


COMMANDS = [
    "messages-segmented",
    "segment-identity-daily",
    "segment-vessel-daily",
    "segment-vessel",
    "segment-info",
    "vessel-info",
]


@pytest.mark.parametrize("command", COMMANDS)
def test_every_command_has_a_help(command: str, capsys: pytest.CaptureFixture) -> None:
    """Checks that every command is registered and describes itself."""
    with pytest.raises(SystemExit) as exit_info:
        main.run([command, "--help"])

    assert exit_info.value.code == 0
    assert command in capsys.readouterr().out


def test_the_log_file_option_writes_the_logs(mocker: MockerFixture, tmp_path: os.PathLike) -> None:
    """Checks that the built-in logging options work with a command."""
    mocker.patch("pipe_segment.cli.commands.vessel_info.main.run")
    log_file = os.path.join(tmp_path, "vessel_info.log")

    main.run(
        [
            "vessel-info",
            "-v",
            "--log-file",
            log_file,
            "--bq-in-segment-identity-daily",
            "p.d.identity",
            "--bq-in-segment-vessel-daily",
            "p.d.vessel",
            "--bq-out-vessel-info",
            "p.d.out",
            "--project",
            "test-project",
            "--labels",
            "environment=test",
        ]
    )

    assert os.path.exists(log_file)
