"""Tests for the entry point of the CLI."""

import pytest

from pytest_mock import MockerFixture

from pipe_segment.cli import main


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
