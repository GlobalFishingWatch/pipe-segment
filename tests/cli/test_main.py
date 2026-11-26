"""Tests for the CLI as a whole."""

import pytest


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
