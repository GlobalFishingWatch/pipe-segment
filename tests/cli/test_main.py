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


@pytest.mark.parametrize("result", [1, 2])
def test_main_exits_with_the_exit_code_of_the_command(mocker: MockerFixture, result: int) -> None:
    """Checks that a command that returns a non-zero exit code fails the process."""
    mocker.patch.object(main, "run", return_value=result)

    with pytest.raises(SystemExit) as exc_info:
        main.main()

    assert exc_info.value.code == result


def test_main_exits_successfully_if_the_command_returns_zero(mocker: MockerFixture) -> None:
    """Checks that a command that returns 0 exits with code 0."""
    mocker.patch.object(main, "run", return_value=0)

    with pytest.raises(SystemExit) as exc_info:
        main.main()

    assert exc_info.value.code == 0


def test_main_returns_normally_if_the_command_has_no_exit_code(mocker: MockerFixture) -> None:
    """Checks that a command that returns nothing does not call sys.exit."""
    mocker.patch.object(main, "run", return_value=None)

    main.main()
